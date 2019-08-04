use postgres::{Client, NoTls, Row, Statement};

const GET_DATA: &str = "select symbol, chart, stats from equity_data where symbol = $1";
const GET_ALL_DATA: &str = "select symbol, chart, stats from equity_data where data_source = $1 and data_source_version = $2 ORDER BY symbol";
const INSERT: &str = "insert into equity_momentum (symbol, strategy, score, fip) values ($1, $2, $3, $4) on conflict on constraint unq_strategy_symbol do update set score=EXCLUDED.score, fip=EXCLUDED.fip";
const GET_DATA_COUNT: &str = "select count(*) as count from equity_data where data_source = $1 and data_source_version = $2";

pub struct Datasource<'a> {
    pub source: &'a str,
    pub version: &'a str,
}

pub struct Momentum<'a> {
    pub symbol: String,
    pub strategy: &'a str,
    pub score: f64,
    pub fip: f64
}

pub struct Database {
    pub conn: Client,
    pub insert: Statement,
    pub all: Statement,
    pub query: Statement
}

impl Database {
    pub fn new() -> Result<Database, &'static str> {
        let mut conn = match Client::connect("host=localhost user=postgres password=mysecretpassword port=5432", NoTls) {
            Ok(conn) => {
                conn
            },
            Err(e) => panic!("Errored out creating database connection for {:?}", e)
        };

        let all = match conn.prepare(GET_ALL_DATA) {
            Ok(stmt) => stmt,
            Err(e) => panic!("Errored out preparing all {:?}",e)
        };

        let query = match conn.prepare(GET_DATA) {
            Ok(stmt) => stmt,
            Err(e) => panic!("Errored out preparing query {:?}",e)
        };

        let insert = match conn.prepare(INSERT) {
            Ok(stmt) => stmt,
            Err(e) => panic!("Errored out preparing insert {:?}",e)
        };

        Ok(Database { conn, insert, all, query  } )
    }

    // TODO add where filter
    fn page_iterator<'a>(&'a mut self, count_query: &'a str, query: &'a str, data_source: &'a Datasource,  page_size: u64) -> Result<QueryIterator<'a>, &'a str> {
        QueryIterator::new(count_query, query, data_source, page_size, &mut self.conn)
    }

    pub fn loop_through_equity_data<'a>(&'a mut self, datasource: &'a Datasource, size: u64) -> QueryIterator<'a> {
        match self.page_iterator(GET_DATA_COUNT, GET_ALL_DATA, datasource,size) {
            Ok(q) => q,
            Err(e) => panic!("Failed to create iterator with error {:?}",e),
        }
    }

    pub fn insert_momentum<'a, 'e>(&mut self, symbol: &'a str, strategy: &'a str, score: f64, fip: f64) -> u64 {
        self.conn.execute(&self.insert, &[&symbol, &strategy, &score, &fip]).unwrap()
    }

    pub fn get_symbol_data<'a>(&mut self, symbol: &'a str) -> Option<Vec<Row>> {
        match self.conn.query(&self.query, &[&symbol]) {
            Ok(rows) => Some(rows),
            Err(e) => {
                println!("failed to get symbol {} with error {}",symbol, e);
                None
            }
        }
    }
}


pub struct QueryIterator<'a> {
    query: &'a str,
    count: u64,
    page_size: u64,
    current_page: u64,
    client: &'a mut Client,
    datasource: &'a Datasource<'a>
}

impl<'a> QueryIterator<'a> {
    pub fn new<'b>(count_query: &'b str, query: &'b str, datasource: &'b Datasource, page_size: u64, client: &'b mut Client) -> Result<QueryIterator<'b>, &'b str> {
        let rows: Vec<Row> = match client.query(count_query as &str, &[&datasource.source, &datasource.version]) {
            Ok(rows) => rows,
            Err(e) => panic!("Errored getting initial count {:?}", e),
        };

        let results_count: i64 = rows.get(0).unwrap().get(0);
        Ok( QueryIterator{ count: results_count as u64, client, datasource, page_size, query, current_page: 0 } )
    }
}


impl<'a> Iterator for QueryIterator<'a> {
    type Item = Vec<Row>;

    fn next(&mut self) -> Option<Vec<Row>> {
        let offset = self.current_page * self.page_size;
        let formatted_query =  &format!("{} LIMIT {} OFFSET {}", self.query, self.page_size, offset)[..];
        self.current_page = self.current_page + 1;
        if offset < self.count {
            match self.client.query(formatted_query, &[&self.datasource.source, &self.datasource.version]) {
                Ok(r) => Some(r),
                Err(e) => {
                    println!("Error getting next page {:?}", e);
                    None
                },
            }
        } else {
            None
        }
    }
}


#[cfg(test)]
mod tests {
    use crate::database::{Database, Datasource};
    #[test]
    fn it_works() {
        match Database::new() {
            Err(e) => panic!(e),
            Ok(_d) => println!("I made it!"),
        }
    }

    #[test]
    fn query_test() {

        match Database::new() {
            Err(e) => panic!(e),
            Ok(mut d) => {
                let v: &str = "v1";
                let s: &str = "AMERITRADE";
                for row in  d.conn.query(&d.all,&[&v, &s]).unwrap() {
                    let _symbol: &str = row.get(0);
                    break;
                }

            },
        }
    }

    #[test]
    fn page_test() {
        match Database::new() {
            Err(e) => panic!(e),
            Ok(mut d) => {
                let data_source: Datasource = Datasource {version:"v1", source:"AMERITRADE"};
                for rows in d.loop_through_equity_data(&data_source, 1) {
                    for row in rows {
                        let _symbol: String = row.get(0);
                    }
                }
            }
        }
    }

    #[test]
    fn insert_upsert_test() {
        match Database::new() {
            Err(e) => panic!(e),
            Ok(mut d) => {
                let symbol: &str = "unittestsymbol";
                let strategy: &str = "unitteststrategy";
                let score_one: f64 = 3.0;
                let fip_one: f64 = 40.3;
                d.insert_momentum(symbol, strategy, score_one, fip_one);
                let fip_two: f64 = 34.0;
                let score_two: f64 = 1.0;
                d.insert_momentum(symbol, strategy, score_two, fip_two);
            }
        }
    }
}
