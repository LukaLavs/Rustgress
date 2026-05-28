use std::iter::Peekable;
use std::str::Chars;

// različni ukazi ki jih parser lahko prebere
#[derive(Debug, PartialEq, Clone)]
pub enum SQLStatement {
    CreateTable {
        name: String,
        columns: Vec<ColumnDef>,
        if_not_exists: bool,
    },
    Insert {
        table_name: String,
        columns: Option<Vec<String>>,
        values: Vec<Vec<SQLValue>>,
    },
    Select {
        columns: Vec<SelectColumn>,
        table_name: String,
        where_clause: Option<WhereClause>,
        order_by: Option<OrderBy>,
        limit: Option<u32>,
    },
    DropTable {
        name: String,
        if_exists: bool,
    },
    Delete {
        table_name: String,
        where_clause: Option<WhereClause>,
    },
    Update {
        table_name: String,
        assignments: Vec<(String, Expression)>,
        where_clause: Option<WhereClause>,
    },
    BeginTransaction,
    Commit,
    Rollback,
}

//definiranje stolpcev

#[derive(Debug, PartialEq, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataTypeDef,
    pub constraints: Vec<ColumnConstraint>,
}

#[derive(Debug, PartialEq, Clone)]
pub enum DataTypeDef {
    Int,
    Integer,
    SmallInt,
    BigInt,
    Varchar(Option<usize>),
    Text,
    Boolean,
    Float,
    Double,
    Date,
    Timestamp,
}

#[derive(Debug, PartialEq, Clone)]
pub enum ColumnConstraint {
    NotNull,
    Unique,
    PrimaryKey,
    Default(SQLValue),
}
//konec def. stolpcev

//def elementi:

#[derive(Debug, PartialEq, Clone)]
pub enum Expression {
    Column(String),
    Literal(SQLValue),
    BinaryOp(Box<Expression>, BinaryOperator, Box<Expression>),
    ComparisonOp(Box<Expression>, ComparisonOperator, Box<Expression>),
}

//def operatorje
#[derive(Debug, PartialEq, Clone)]
pub enum BinaryOperator {
    And,
    Or,
    Add,
    Sub,
    Mul,
    Div,
}

#[derive(Debug, PartialEq, Clone)]
pub enum ComparisonOperator {
    Eq,
    Ne,
    Lt,
    Gt,
    Lte,
    Gte,
    Like,
    In,
    Between,
}
//end def operator

#[derive(Debug, PartialEq, Clone)]
pub enum SelectColumn {
    All,
    Expression(Expression, Option<String>), // expr, alias
}

#[derive(Debug, PartialEq, Clone)]
pub struct OrderByField {
    pub column: String,
    pub descending: bool,
}

#[derive(Debug, PartialEq, Clone)]
pub struct OrderBy {
    pub fields: Vec<OrderByField>, // Sedaj imamo seznam stolpcev za sortiranje!
}

#[derive(Debug, PartialEq, Clone)]
pub enum SQLValue {
    Integer(i64),
    String(String),
    Boolean(bool),
    Float(f64),
    Null,
}

pub struct SQLParser<'a> {
    chars: Peekable<Chars<'a>>,
    position: usize,
}

#[derive(Debug, PartialEq, Clone)]
pub struct WhereClause {
    pub condition: Expression,
}

impl<'a> SQLParser<'a> {
    pub fn skip_whitespace(&mut self) {
        while let Some(&c) = self.chars.peek() {
            if c.is_whitespace() {
                self.position += 1;
                self.chars.next();
            } else {
                break;
            }
        }
    }

    pub fn peek_char(&self, c: char) -> bool {
        self.chars.clone().peek() == Some(&c)
    }


    pub fn new(sql: &'a str) -> Self {
        SQLParser {
            chars: sql.chars().peekable(),
            position: 0,
        }
    }
    pub fn parse_statement(&mut self) -> Result<SQLStatement, String> {
    self.skip_whitespace();

    let keyword = self.parse_keyword()?.to_lowercase();

    match keyword.as_str() {
        "create" => {
            self.parse_create_table()
        }
        "select" => self.parse_select(),
        "insert" => self.parse_insert(),
        "drop" => self.parse_drop_table(),
        "delete" => self.parse_delete(),
        "update" => self.parse_update(),
        "begin" => {
            self.expect_keyword("transaction")?;
            Ok(SQLStatement::BeginTransaction)
        }
        "commit" => Ok(SQLStatement::Commit),
        "rollback" => Ok(SQLStatement::Rollback),
        _ => Err(format!("Unknown statement: {}", keyword)),
    }
}

    pub fn parse_insert(&mut self) -> Result<SQLStatement, String> {
        self.skip_whitespace();  

        self.expect_keyword("into")?;

        let table_name = self.parse_identifier()?;

        self.skip_whitespace();

        let columns = if self.peek_char('(') {
            self.expect_char('(')?;

            let mut cols = Vec::new();

            loop {
                let col = self.parse_identifier()?;
                cols.push(col);

                self.skip_whitespace();

                if self.peek_char(',') {
                    self.chars.next();
                    self.position += 1;
                    self.skip_whitespace();  
                } else {
                    break;
                }
            }

            self.expect_char(')')?;
            Some(cols)
        } else {
            None
        };

        self.skip_whitespace(); 
        self.expect_keyword("values")?;
        self.skip_whitespace();  
        self.expect_char('(')?;

        let mut values = Vec::new();
        let mut row_values = Vec::new();

        loop {
            let value = self.parse_sql_value()?;
            row_values.push(value);

            self.skip_whitespace();

            if self.peek_char(',') {
                self.chars.next();
                self.position += 1;
                self.skip_whitespace();  
            } else {
                break;
            }
        }

        self.expect_char(')')?;
        values.push(row_values);

        self.skip_whitespace();
        while self.peek_char(',') {
            self.chars.next();
            self.position += 1;
            self.skip_whitespace();  

            self.expect_char('(')?;

            let mut more_row_values = Vec::new();

            loop {
                let value = self.parse_sql_value()?;
                more_row_values.push(value);

                self.skip_whitespace();

                if self.peek_char(',') {
                    self.chars.next();
                    self.position += 1;
                    self.skip_whitespace(); 
                } else {
                    break;
                }
            }

            self.expect_char(')')?;
            values.push(more_row_values);

            self.skip_whitespace();
        }

        Ok(SQLStatement::Insert {
            table_name,
            columns,
            values,
        })
    }


    pub fn parse_create_table(&mut self) -> Result<SQLStatement, String> {
        self.expect_keyword("table")?;

        let if_not_exists = self.peek_keyword("if")?;
        if if_not_exists {
            self.parse_keyword()?;
            self.expect_keyword("not")?;
            self.expect_keyword("exists")?;
        }

        let table_name = self.parse_identifier()?;
        self.expect_char('(')?;

        let mut columns = Vec::new();

        loop {
            self.skip_whitespace();

            if self.peek_char(')') {
                self.chars.next();
                self.position += 1;
                break;
            }

            let col_name = self.parse_identifier()?;
            let data_type = self.parse_data_type()?;

            let constraints = self.parse_column_constraints()?;

            columns.push(ColumnDef {
                name: col_name,
                data_type,
                constraints,
            });

            self.skip_whitespace();

            if self.peek_char(',') {
                self.chars.next();
                self.position += 1;
            }
        }

        Ok(SQLStatement::CreateTable {
            name: table_name,
            columns,
            if_not_exists,
        })
    }

    // POPRAVLJENO
    pub fn parse_select(&mut self) -> Result<SQLStatement, String> {
        self.skip_whitespace();

        let columns = if self.peek_char('*') {
            self.chars.next();
            self.position += 1;
            vec![SelectColumn::All]
        } else {
            let mut cols = Vec::new();

            loop {
                let ident = self.parse_identifier()?;

                cols.push(SelectColumn::Expression(
                    Expression::Column(ident),
                    None,
                ));

                self.skip_whitespace();

                if self.peek_char(',') {
                    self.chars.next();
                    self.position += 1;
                } else {
                    break;
                }
            }

            cols
        };

        self.expect_keyword("from")?;
        let table_name = self.parse_identifier()?;

        let where_clause = if self.peek_keyword("where")? {
            self.parse_keyword()?;
            Some(self.parse_where_clause()?)
        } else {
            None
        };

        let order_by = if self.peek_keyword("order")? {
            self.parse_keyword()?; // ORDER
            self.expect_keyword("by")?;

            let mut fields = Vec::new();

            loop {
                let column = self.parse_identifier()?;
                self.skip_whitespace();

                let descending = if self.peek_keyword("desc")? {
                    self.parse_keyword()?;
                    true
                } else if self.peek_keyword("asc")? {
                    self.parse_keyword()?;
                    false
                } else {
                    false // Privzeto je ASC
                };

                fields.push(OrderByField { column, descending });

                self.skip_whitespace();
                if self.peek_char(',') {
                    self.chars.next();
                    self.position += 1;
                } else {
                    break;
                }
            }

            Some(OrderBy { fields })
        } else {
            None
        };

        let limit = if self.peek_keyword("limit")? {
            self.parse_keyword()?;
            Some(self.parse_number()? as u32)
        } else {
            None
        };

        Ok(SQLStatement::Select {
            columns,
            table_name,
            where_clause,
            order_by,
            limit,
        })
    }

pub fn parse_sql_value(&mut self) -> Result<SQLValue, String> {
    self.skip_whitespace();

    // Najprej preveri če je številka
    if let Some(&c) = self.chars.peek() {
        if c == '-' || c.is_ascii_digit() {
            return self.parse_numeric_value();
        }
    }

    // Preveri če je string
    if self.peek_char('\'') {
        self.chars.next(); // požri '
        self.position += 1;

        let mut value = String::new();

        while let Some(c) = self.chars.next() {
            self.position += 1;

            if c == '\'' {
                // Escaping: dva apostrofa pomenita en apostrof
                if self.chars.peek() == Some(&'\'') {
                    value.push('\'');
                    self.chars.next();
                    self.position += 1;
                    continue;
                }
                break;
            }
            value.push(c);
        }

        return Ok(SQLValue::String(value));
    }

    // Preveri če je ključna beseda (TRUE, FALSE, NULL)
    if let Some(&c) = self.chars.peek() {
        if c.is_alphabetic() {
            let keyword = self.parse_keyword()?;
            match keyword.to_lowercase().as_str() {
                "true" => return Ok(SQLValue::Boolean(true)),
                "false" => return Ok(SQLValue::Boolean(false)),
                "null" => return Ok(SQLValue::Null),
                _ => return Err(format!("Invalid boolean/null value: {}", keyword)),
            }
        }
    }

    Err("Unexpected SQL value".to_string())
}
    // POPRAVLJENO
    pub fn parse_number(&mut self) -> Result<i64, String> {
        self.skip_whitespace();

        let mut number = String::new();

        if self.peek_char('-') {
            number.push('-');
            self.chars.next();
            self.position += 1;
        }

        while let Some(&c) = self.chars.peek() {
            if c.is_ascii_digit() {
                number.push(c);
                self.chars.next();
                self.position += 1;
            } else {
                break;
            }
        }

        number
            .parse::<i64>()
            .map_err(|_| "Invalid number".to_string())
    }

    pub fn parse_numeric_value(&mut self) -> Result<SQLValue, String> {
        self.skip_whitespace();

        let mut value = String::new();
        let mut has_dot = false;

        if self.peek_char('-') {
            value.push('-');
            self.chars.next();
            self.position += 1;
        }

        while let Some(&c) = self.chars.peek() {
            if c.is_ascii_digit() {
                value.push(c);
                self.chars.next();
                self.position += 1;
            } else if c == '.' {
                has_dot = true;
                value.push(c);
                self.chars.next();
                self.position += 1;
            } else {
                break;
            }
        }

        if has_dot {
            value
                .parse::<f64>()
                .map(SQLValue::Float)
                .map_err(|_| "Invalid float".to_string())
        } else {
            value
                .parse::<i64>()
                .map(SQLValue::Integer)
                .map_err(|_| "Invalid integer".to_string())
        }
    }

    pub fn expect_keyword(&mut self, kw: &str) -> Result<(), String> {
        let parsed = self.parse_keyword()?;

        if parsed.to_lowercase() == kw.to_lowercase() {
            Ok(())
        } else {
            Err(format!("Expected keyword {}", kw))
        }
    }

    pub fn peek_keyword(&mut self, kw: &str) -> Result<bool, String> {
        let saved_chars = self.chars.clone();
        let saved_pos = self.position;

        let result = match self.parse_keyword() {
            Ok(word) => word.to_lowercase() == kw.to_lowercase(),
            Err(_) => false,
        };

        self.chars = saved_chars;
        self.position = saved_pos;

        Ok(result)
    }
pub fn parse_keyword(&mut self) -> Result<String, String> {
    self.skip_whitespace(); 

    let mut keyword = String::new();

    // Beri  brez presledkov
    while let Some(&c) = self.chars.peek() {
        if c.is_alphanumeric() || c == '_' {
            keyword.push(c);
            self.chars.next();
            self.position += 1;
        } else {
            break;  // Ko naletimo na presledek ali drug znak, končamo
        }
    }

    if keyword.is_empty() {
        Err("Expected keyword".to_string())
    } else {
        Ok(keyword)
    }
}
    pub fn parse_drop_table(&mut self) -> Result<SQLStatement, String> {
        self.skip_whitespace();
        self.expect_keyword("table")?;
        
        let if_exists = self.peek_keyword("if")?;
        if if_exists {
            self.parse_keyword()?; // IF
            self.expect_keyword("exists")?;
        }
        
        let table_name = self.parse_identifier()?;
        
        Ok(SQLStatement::DropTable {
            name: table_name,
            if_exists,
        })
    }

    pub fn parse_delete(&mut self) -> Result<SQLStatement, String> {
        self.skip_whitespace();
        self.expect_keyword("from")?;
        let table_name = self.parse_identifier()?;
        self.skip_whitespace();
        let where_clause = if self.peek_keyword("where")? {
            self.parse_keyword()?; // Požri besedo "WHERE"
            Some(self.parse_where_clause()?)
        } else {
            None
        };
        Ok(SQLStatement::Delete {
            table_name,
            where_clause,
        })
    }

    pub fn parse_update(&mut self) -> Result<SQLStatement, String> {
        // Parsa: UPDATE table_name SET col1 = val1, col2 = val2 WHERE 
        let table_name = self.parse_identifier()?;

        self.expect_keyword("set")?;

        let mut assignments = Vec::new();

        // Parsanje SET  : col = expr, col2 = expr2
        loop {
            let col_name = self.parse_identifier()?;
            self.expect_char('=')?;
            
            let val = self.parse_sql_value()?;
            assignments.push((col_name, Expression::Literal(val)));

            self.skip_whitespace();
            if self.peek_char(',') {
                self.chars.next();
                self.position += 1;
            } else {
                break;
            }
        }

        // Parsanje WHERE 
        let where_clause = if self.peek_keyword("where")? {
            self.parse_keyword()?; // Požri "WHERE"
            Some(self.parse_where_clause()?)
        } else {
            None
        };

        Ok(SQLStatement::Update {
            table_name,
            assignments,
            where_clause,
        })
    }

    pub fn parse_identifier(&mut self) -> Result<String, String> {
        self.skip_whitespace();

        let mut ident = String::new();

        while let Some(&c) = self.chars.peek() {
            if c.is_alphanumeric() || c == '_' {
                ident.push(c);
                self.chars.next();
                self.position += 1;
            } else {
                break;
            }
        }

        if ident.is_empty() {
            Err("Expected identifier".to_string())
        } else {
            Ok(ident)
        }
    }

    pub fn expect_char(&mut self, expected: char) -> Result<(), String> {
        self.skip_whitespace();

        match self.chars.next() {
            Some(c) if c == expected => {
                self.position += 1;
                Ok(())
            }
            _ => Err(format!("Expected '{}'", expected)),
        }
    }

    pub fn parse_data_type(&mut self) -> Result<DataTypeDef, String> {
        let data_type = self.parse_keyword()?.to_lowercase();

        match data_type.as_str() {
            "int" => Ok(DataTypeDef::Int),
            "integer" => Ok(DataTypeDef::Integer),
            "smallint" => Ok(DataTypeDef::SmallInt),
            "bigint" => Ok(DataTypeDef::BigInt),
            "text" => Ok(DataTypeDef::Text),
            "boolean" => Ok(DataTypeDef::Boolean),
            "float" => Ok(DataTypeDef::Float),
            "double" => Ok(DataTypeDef::Double),
            "date" => Ok(DataTypeDef::Date),
            "timestamp" => Ok(DataTypeDef::Timestamp),
            "varchar" => Ok(DataTypeDef::Varchar(None)),
            _ => Err(format!("Unknown data type: {}", data_type)),
        }
    }

    pub fn parse_column_constraints(&mut self) -> Result<Vec<ColumnConstraint>, String> {
        let mut constraints = Vec::new();

        loop {
            if self.peek_keyword("primary")? {
                self.parse_keyword()?;
                self.expect_keyword("key")?;
                constraints.push(ColumnConstraint::PrimaryKey);
            } else if self.peek_keyword("not")? {
                self.parse_keyword()?;
                self.expect_keyword("null")?;
                constraints.push(ColumnConstraint::NotNull);
            } else if self.peek_keyword("unique")? {
                self.parse_keyword()?;
                constraints.push(ColumnConstraint::Unique);
            } else {
                break;
            }
        }

        Ok(constraints)
    }

    // ======================= WHERE CLAUSE PARSER =======================
    pub fn parse_where_clause(&mut self) -> Result<WhereClause, String> {
        let condition = self.parse_or_expression()?;
        Ok(WhereClause { condition })
    }

    // 1. Nivo: OR
    fn parse_or_expression(&mut self) -> Result<Expression, String> {
        let mut expr = self.parse_and_expression()?;
        loop {
            self.skip_whitespace();
            if self.peek_keyword("or")? {
                self.parse_keyword()?; // Požri "OR"
                let right = self.parse_and_expression()?;
                expr = Expression::BinaryOp(Box::new(expr), BinaryOperator::Or, Box::new(right));
            } else {
                break;
            }
        }
        Ok(expr)
    }

    // 2. Nivo: AND
    fn parse_and_expression(&mut self) -> Result<Expression, String> {
        let mut expr = self.parse_comparison_expression()?;
        loop {
            self.skip_whitespace();
            if self.peek_keyword("and")? {
                self.parse_keyword()?; // Požri "AND"
                let right = self.parse_comparison_expression()?;
                expr = Expression::BinaryOp(Box::new(expr), BinaryOperator::And, Box::new(right));
            } else {
                break;
            }
        }
        Ok(expr)
    }

    // 3. Nivo: Primerjalni operatorji (=, !=, <, >, <=, >=)
    fn parse_comparison_expression(&mut self) -> Result<Expression, String> {
        let left = self.parse_additive_expression()?;
        self.skip_whitespace();
        if let Some(&c) = self.chars.peek() {
            if c == '=' || c == '!' || c == '<' || c == '>' {
                let op = match self.chars.next() {
                    Some('=') => { self.position += 1; ComparisonOperator::Eq }
                    Some('!') => {
                        self.position += 1;
                        self.expect_char('=')?;
                        ComparisonOperator::Ne
                    }
                    Some('<') => {
                        self.position += 1;
                        if self.peek_char('=') {
                            self.chars.next(); self.position += 1;
                            ComparisonOperator::Lte
                        } else if self.peek_char('>') {
                            self.chars.next(); self.position += 1;
                            ComparisonOperator::Ne
                        } else {
                            ComparisonOperator::Lt
                        }
                    }
                    Some('>') => {
                        self.position += 1;
                        if self.peek_char('=') {
                            self.chars.next(); self.position += 1;
                            ComparisonOperator::Gte
                        } else {
                            ComparisonOperator::Gt
                        }
                    }
                    _ => return Err("Invalid comparison state".to_string()),
                };
                let right = self.parse_additive_expression()?;
                return Ok(Expression::ComparisonOp(Box::new(left), op, Box::new(right)));
            }
        }
        Ok(left)
    }

    // 4. Nivo: Seštevanje in odštevanje (+, -)
    fn parse_additive_expression(&mut self) -> Result<Expression, String> {
        let mut expr = self.parse_multiplicative_expression()?;
        loop {
            self.skip_whitespace();
            if self.peek_char('+') {
                self.chars.next(); self.position += 1;
                let right = self.parse_multiplicative_expression()?;
                expr = Expression::BinaryOp(Box::new(expr), BinaryOperator::Add, Box::new(right));
            } else if self.peek_char('-') {
                let mut cloned = self.chars.clone();
                cloned.next();
                if let Some(&next_c) = cloned.peek() {
                    if next_c.is_whitespace() || next_c.is_alphabetic() {
                        self.chars.next(); self.position += 1;
                        let right = self.parse_multiplicative_expression()?;
                        expr = Expression::BinaryOp(Box::new(expr), BinaryOperator::Sub, Box::new(right));
                        continue;
                    }
                }
                break;
            } else {
                break;
            }
        }
        Ok(expr)
    }

    // 5. Nivo: Množenje in deljenje (*, /) -> Višja prioriteta kot seštevanje!
    fn parse_multiplicative_expression(&mut self) -> Result<Expression, String> {
        let mut expr = self.parse_primary()?;
        loop {
            self.skip_whitespace();
            if self.peek_char('*') {
                self.chars.next(); self.position += 1;
                let right = self.parse_primary()?;
                expr = Expression::BinaryOp(Box::new(expr), BinaryOperator::Mul, Box::new(right));
            } else if self.peek_char('/') {
                self.chars.next(); self.position += 1;
                let right = self.parse_primary()?;
                expr = Expression::BinaryOp(Box::new(expr), BinaryOperator::Div, Box::new(right));
            } else {
                break;
            }
        }
        Ok(expr)
    }

    // 6. Nivo: Osnovni elementi (Stolpci, Literali)
    fn parse_primary(&mut self) -> Result<Expression, String> {
        self.skip_whitespace();
        if let Some(&c) = self.chars.peek() {
            if c.is_alphabetic() {
                let keyword = self.parse_keyword()?;
                match keyword.to_lowercase().as_str() {
                    "true" => return Ok(Expression::Literal(SQLValue::Boolean(true))),
                    "false" => return Ok(Expression::Literal(SQLValue::Boolean(false))),
                    "null" => return Ok(Expression::Literal(SQLValue::Null)),
                    _ => return Ok(Expression::Column(keyword)),
                }
            } else if c == '\'' || c == '-' || c.is_ascii_digit() {
                let val = self.parse_sql_value()?;
                return Ok(Expression::Literal(val));
            }
        }
        Err("Expected column name or literal value".to_string())
    }
}

impl<'a> SQLParser<'a> {
    pub fn parse_script(&mut self) -> Result<Vec<SQLStatement>, String> {
        let mut statements = Vec::new();
        while !self.is_eof() {
            self.skip_whitespace();
            if self.is_eof() { break; }
            statements.push(self.parse_statement()?);
            self.skip_whitespace();
            if self.peek_char(';') {
                self.chars.next(); // takes ';'
                self.position += 1;
            }
        }
        Ok(statements)
    }

    pub fn is_eof(&self) -> bool {
        self.chars.clone().peek().is_none()
    }
}
