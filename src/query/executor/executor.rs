// =========================================================================
// ./query/executor.rs
// =========================================================================

use std::sync::{Arc, RwLock};
use crate::storage::buffer::manager::BufferPoolManager;
use crate::storage::disk::manager::Table;
use crate::access::transaction::manager::{TransactionManager};
use crate::access::heap::scan::HeapScan;
use crate::access::tuple::desc::TupleDescriptor;
use crate::access::tuple::tuple::{HeapTuple};
use crate::catalog::manager::CatalogManager;
use crate::catalog::types::Value;
use crate::query::parser::parser::*;
use crate::common::types::TransactionId;

/// Osnovni vmesnik za vse operatorje v izvajalnem načrtu (Volcano Iterator Model).
pub trait Executor {
    /// Pripravi operator in njegove pod-operatorje na izvajanje.
    fn init(&mut self);
    
    /// Vrne naslednjo vrstico (Tuple) ali `None`, ko ni več podatkov.
    fn next(&mut self) -> Option<HeapTuple>;
    
    /// Vrne shemo izhodnih podatkov, ki jih generira ta operator.
    fn get_tuple_desc(&self) -> Arc<TupleDescriptor>;
}

// =========================================================================
// 1. SEQ SCAN EXECUTOR (Pregled celotne tabele)
// =========================================================================
pub struct SeqScanExecutor {
    bpm: Arc<BufferPoolManager>,
    tm: Arc<TransactionManager>,
    table_handle: Arc<RwLock<Table>>,
    schema: Arc<TupleDescriptor>,
    scan: Option<HeapScan>,
}

impl SeqScanExecutor {
    pub fn new(
        bpm: Arc<BufferPoolManager>,
        tm: Arc<TransactionManager>,
        table_handle: Arc<RwLock<Table>>,
        schema: Arc<TupleDescriptor>,
    ) -> Self {
        Self {
            bpm,
            tm,
            table_handle,
            schema,
            scan: None,
        }
    }
}

impl Executor for SeqScanExecutor {
    fn init(&mut self) {
        // Inicializiramo HeapScan, ki bo iteriral čez strani na disku/v buffer poolu
        self.scan = Some(HeapScan::new(
            self.bpm.clone(),
            self.table_handle.clone(),
            self.tm.clone(),
        ));
    }

    fn next(&mut self) -> Option<HeapTuple> {
        // Vzamemo naslednji element iz nizkonivojskega HeapScan-a
        if let Some(ref mut scan_iter) = self.scan {
            scan_iter.next()
        } else {
            None
        }
    }

    fn get_tuple_desc(&self) -> Arc<TupleDescriptor> {
        self.schema.clone()
    }
}

// =========================================================================
// 2. FILTER EXECUTOR (Filtriranje vrstic glede na WHERE klavzulo)
// =========================================================================

pub struct FilterExecutor {
    child: Box<dyn Executor>,
    expression: Expression,
}

impl FilterExecutor {
    pub fn new(child: Box<dyn Executor>, expression: Expression) -> Self {
        Self { child, expression }
    }

    /// Pomagalo za rekurzivno pretvorbo in izračun izrazov v `Value` tip baze.
    fn eval_expr(&self, expr: &Expression, values: &[Value], desc: &TupleDescriptor) -> Value {
        match expr {
            Expression::Column(col_name) => {
                if let Some(idx) = desc.columns.iter().position(|c| &c.name == col_name) {
                    values[idx].clone()
                } else {
                    Value::Null
                }
            }
            Expression::Literal(sql_val) => {
                // Pretvori SQLValue iz parserja v Value tip, ki ga uporablja tvoja baza
                match sql_val {
                    SQLValue::Integer(num) => Value::Integer(*num as i32), // Pazi na i64 -> i32 pretvorbo po potrebi
                    SQLValue::String(s) => Value::Varchar(s.clone()),
                    SQLValue::Boolean(b) => Value::Boolean(*b),
                    SQLValue::Float(f) => Value::Float(*f as f32),
                    SQLValue::Null => Value::Null,
                }
            }
            Expression::BinaryOp(left, op, right) => {
                let left_val = self.eval_expr(left, values, desc);
                let right_val = self.eval_expr(right, values, desc);
                self.eval_binary_op(&left_val, op, &right_val)
            }
            Expression::ComparisonOp(left, op, right) => {
                let left_val = self.eval_expr(left, values, desc);
                let right_val = self.eval_expr(right, values, desc);
                if self.eval_comparison(&left_val, op, &right_val) {
                    Value::Boolean(true)
                } else {
                    Value::Boolean(false)
                }
            }
        }
    }

    /// Izvajanje logičnih in matematičnih binarnih operacij (AND, OR, +, -, *, /)
    fn eval_binary_op(&self, left: &Value, op: &BinaryOperator, right: &Value) -> Value {
        match op {
            BinaryOperator::And => {
                if let (Value::Boolean(l), Value::Boolean(r)) = (left, right) {
                    Value::Boolean(*l && *r)
                } else {
                    Value::Null
                }
            }
            BinaryOperator::Or => {
                if let (Value::Boolean(l), Value::Boolean(r)) = (left, right) {
                    Value::Boolean(*l || *r)
                } else {
                    Value::Null
                }
            }
            BinaryOperator::Add => {
                if let (Value::Integer(l), Value::Integer(r)) = (left, right) {
                    Value::Integer(l + r)
                } else {
                    Value::Null
                }
            }
            BinaryOperator::Sub => {
                if let (Value::Integer(l), Value::Integer(r)) = (left, right) {
                    Value::Integer(l - r)
                } else {
                    Value::Null
                }
            }
            BinaryOperator::Mul => {
                if let (Value::Integer(l), Value::Integer(r)) = (left, right) {
                    Value::Integer(l * r)
                } else {
                    Value::Null
                }
            }
            BinaryOperator::Div => {
                if let (Value::Integer(l), Value::Integer(r)) = (left, right) {
                    if *r == 0 { Value::Null } else { Value::Integer(l / r) }
                } else {
                    Value::Null
                }
            }
        }
    }

    /// Izvajanje izključno primerjalnih operacij z ločenim ComparisonOperator enumom
    fn eval_comparison(&self, left: &Value, op: &ComparisonOperator, right: &Value) -> bool {
        match (left, right) {
            (Value::Integer(l), Value::Integer(r)) => match op {
                ComparisonOperator::Eq => l == r,
                ComparisonOperator::Ne => l != r,
                ComparisonOperator::Lt => l < r,
                ComparisonOperator::Gt => l > r,
                ComparisonOperator::Lte => l <= r,
                ComparisonOperator::Gte => l >= r,
                _ => false,
            },
            (Value::Varchar(l), Value::Varchar(r)) => match op {
                ComparisonOperator::Eq => l == r,
                ComparisonOperator::Ne => l != r,
                ComparisonOperator::Lt => l < r,
                ComparisonOperator::Gt => l > r,
                ComparisonOperator::Lte => l <= r,
                ComparisonOperator::Gte => l >= r,
                _ => false,
            },
            (Value::Boolean(l), Value::Boolean(r)) => match op {
                ComparisonOperator::Eq => l == r,
                ComparisonOperator::Ne => l != r,
                _ => false,
            },
            _ => false, // Če se tipa ne ujemata, vrnemo false
        }
    }

    /// Glavna filtrirna funkcija, ki preveri, ali vrstica ustreza pogoju WHERE klavzule
    fn evaluate(&self, tuple: &HeapTuple, desc: &TupleDescriptor) -> bool {
        let values = desc.unpack_from_tuple(tuple);
        
        // Strogo ločena match arma za vsak tip operatorja, kar prepreči "mismatched types" napako
        match &self.expression {
            Expression::ComparisonOp(left, op, right) => {
                let left_val = self.eval_expr(left, &values, desc);
                let right_val = self.eval_expr(right, &values, desc);
                self.eval_comparison(&left_val, op, &right_val)
            }
            Expression::BinaryOp(left, op, right) => {
                // Če imamo sestavljen izraz (npr. WHERE pogoj1 AND pogoj2), ga izračunamo
                let res = self.eval_expr(&self.expression, &values, desc);
                match res {
                    Value::Boolean(b) => b,
                    _ => false,
                }
            }
            // Če je v WHERE padel samo surov stolpec ali literal (npr. WHERE 1), preverimo resničnost
            Expression::Literal(SQLValue::Boolean(b)) => *b,
            _ => false,
        }
    }
}

impl Executor for FilterExecutor {
    fn init(&mut self) {
        self.child.init();
    }

    fn next(&mut self) -> Option<HeapTuple> {
        let desc = self.child.get_tuple_desc();
        while let Some(tuple) = self.child.next() {
            if self.evaluate(&tuple, &desc) {
                return Some(tuple);
            }
        }
        None
    }

    fn get_tuple_desc(&self) -> Arc<TupleDescriptor> {
        self.child.get_tuple_desc()
    }
}

// =========================================================================
// 3. PROJECTION EXECUTOR (Izbira specifičnih stolpcev - SELECT col1, col2)
// =========================================================================
pub struct ProjectionExecutor {
    child: Box<dyn Executor>,
    project_columns: Vec<String>,
    output_schema: Arc<TupleDescriptor>,
    xid: TransactionId,
}

impl ProjectionExecutor {
    pub fn new(child: Box<dyn Executor>, project_columns: Vec<String>, xid: TransactionId) -> Self {
        let child_schema = child.get_tuple_desc();
        let mut out_columns = Vec::new();

        // Generiramo novo (zožano) shemo za izhodni operator
        for col_name in &project_columns {
            if let Some(col) = child_schema.columns.iter().find(|c| &c.name == col_name) {
                out_columns.push(col.clone());
            }
        }

        let output_schema = Arc::new(TupleDescriptor::new(out_columns));

        Self {
            child,
            project_columns,
            output_schema,
            xid,
        }
    }
}

impl Executor for ProjectionExecutor {
    fn init(&mut self) {
        self.child.init();
    }

    fn next(&mut self) -> Option<HeapTuple> {
        if let Some(child_tuple) = self.child.next() {
            let child_desc = self.child.get_tuple_desc();
            let all_values = child_desc.unpack_from_tuple(&child_tuple);
            
            let mut projected_values = Vec::new();
            
            // Izberemo samo tiste vrednosti, ki so v projekcijskem seznamu
            for col_name in &self.project_columns {
                if let Some(idx) = child_desc.columns.iter().position(|c| &c.name == col_name) {
                    projected_values.push(all_values[idx].clone());
                }
            }
            
            // Ponovno zapakiramo vrstico z novo shemo
            Some(self.output_schema.pack(projected_values, self.xid))
        } else {
            None
        }
    }

    fn get_tuple_desc(&self) -> Arc<TupleDescriptor> {
        self.output_schema.clone()
    }
}

// =========================================================================
// 4. LIMIT EXECUTOR (Omejitev števila vrstic - LIMIT n)
// =========================================================================
pub struct LimitExecutor {
    child: Box<dyn Executor>,
    limit: usize,
    cursor: usize,
}

impl LimitExecutor {
    pub fn new(child: Box<dyn Executor>, limit: usize) -> Self {
        Self {
            child,
            limit,
            cursor: 0,
        }
    }
}

impl Executor for LimitExecutor {
    fn init(&mut self) {
        self.child.init();
        self.cursor = 0;
    }

    fn next(&mut self) -> Option<HeapTuple> {
        if self.cursor >= self.limit {
            return None;
        }
        
        if let Some(tuple) = self.child.next() {
            self.cursor += 1;
            Some(tuple)
        } else {
            None
        }
    }

    fn get_tuple_desc(&self) -> Arc<TupleDescriptor> {
        self.child.get_tuple_desc()
    }
}


// =========================================================================
// 5. POPRAVLJEN GLAVNI EXECUTOR ENGINE
// =========================================================================

pub struct ExecutionEngine {
    bpm: Arc<BufferPoolManager>,
    sm: Arc<crate::storage::manager::StorageManager>,
    tm: Arc<TransactionManager>,
    cm: Arc<CatalogManager>,
}
 
impl ExecutionEngine {
    pub fn new(
        bpm: Arc<BufferPoolManager>,
        sm: Arc<crate::storage::manager::StorageManager>,
        tm: Arc<TransactionManager>,
        cm: Arc<CatalogManager>,
    ) -> Self {
        Self { bpm, sm, tm, cm }
    }

    /// Sprejme parsano AST poizvedbo in zgradi drevo operatorjev (Execution Plan).
    pub fn execute_statement(&self, statement: SQLStatement, xid: TransactionId) -> (Vec<Vec<Value>>, Arc<TupleDescriptor>) {
        match statement {
            SQLStatement::Select {
                columns,
                table_name,
                where_clause,
                order_by: _,
                limit,
            } => {
                let table_oid = self.cm.get_table_oid(&table_name).expect("Tabela ne obstaja v katalogu!");
                let schema = Arc::new(self.cm.get_schema(table_oid));
                let table_handle = self.sm.get_table(table_oid);

                let mut plan: Box<dyn Executor> = Box::new(SeqScanExecutor::new(
                    self.bpm.clone(),
                    self.tm.clone(),
                    table_handle,
                    schema.clone(), // Pošljemo klonirano izvorno shemo
                ));

                if let Some(where_struct) = where_clause {
                    plan = Box::new(FilterExecutor::new(plan, where_struct.condition));
                }

                let is_select_all = columns.len() == 1 && matches!(columns[0], SelectColumn::All);

                if !is_select_all && !columns.is_empty() {
                    let mut project_names = Vec::new();
                    for col in columns {
                        if let SelectColumn::Expression(Expression::Column(name), _) = col {
                            project_names.push(name);
                        }
                    }
                    if !project_names.is_empty() {
                        plan = Box::new(ProjectionExecutor::new(plan, project_names, xid));
                    }
                }

                if let Some(lim_val) = limit {
                    plan = Box::new(LimitExecutor::new(plan, lim_val as usize));
                }

                plan.init();
                
                // TUKAJ UJAMEMO TOČNO TISTO SHEMO, KI JO IMA ZADNJI OPERATOR NA VRHU NAČRTA
                let output_desc = plan.get_tuple_desc(); 
                let mut results = Vec::new();

                while let Some(tuple) = plan.next() {
                    let unpacked = output_desc.unpack_from_tuple(&tuple);
                    results.push(unpacked);
                }

                // 2. VRNEMO REZULTATE IN TOČNO TO IZBRANO SHEMO
                (results, output_desc)
            }
            _ => {
                // Za ostale ukaze vrne prazen rezultat in prazno shemo
                (Vec::new(), Arc::new(TupleDescriptor::new(Vec::new())))
            }
        }
    }
}