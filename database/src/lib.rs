use postgres::{Client, Statement, NoTls};

struct database {
    conn: Client,
    get_one: Statement,
    get_all_paginginated: Statement
}

const get_data: string = "select * from data where symbol = ?";
const get_data_page: string = "select * from data limit ? offest ?";
const insert_idea: string = "insert into action () values (?, ? , ?, ?)";
const get_data_count: string = "select count(*) from data";



#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
