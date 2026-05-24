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
use crate::access::heap::access::HeapAccess;
use crate::catalog::types::DataType;
use crate::access::tuple;
use std::cmp::Ordering;

pub trait Executor { // Volcano iterator model
    fn init(&mut self);
    fn next(&mut self) -> Option<HeapTuple>;
    fn get_tuple_desc(&self) -> Arc<TupleDescriptor>; // schema of output tuples
}

// =========================================================================
// 1. SEQ SCAN EXECUTOR
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
    fn init(&mut self) { // inicialization of iterator
        self.scan = Some(HeapScan::new(
            self.bpm.clone(),
            self.table_handle.clone(),
            self.tm.clone(),
        ));
    }
    fn next(&mut self) -> Option<HeapTuple> { // next tuple from iterator
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
// 2. FILTER EXECUTOR
// =========================================================================

pub struct FilterExecutor { // recursive filter executor
    child: Box<dyn Executor>,
    expression: Expression,
}

impl FilterExecutor {
    pub fn new(child: Box<dyn Executor>, expression: Expression) -> Self {
        Self { child, expression }
    }
    /// helper function
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
                // TODO: write this in a separate function
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

    /// evaluates binary operations (AND, OR, +, -, *, /)
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

    /// evaluates logical operations with cross-type support for numbers
    fn eval_comparison(&self, left: &Value, op: &ComparisonOperator, right: &Value) -> bool {
        // Najprej preverimo, ali gre za numerično primerjavo (lahko sta mešana Integer in Float)
        match (left, right) {
            // Če je vsaj eden od njiju Float (ali oba), ju primerjamo kot f32 (ali f64, odvisno od tvojega Value enuma)
            (Value::Float(l), Value::Float(r)) => self.compare_floats(*l, *r, op),
            (Value::Float(l), Value::Integer(r)) => self.compare_floats(*l, *r as f32, op),
            (Value::Integer(l), Value::Float(r)) => self.compare_floats(*l as f32, *r, op),

            // Klasična celoštevilska primerjava
            (Value::Integer(l), Value::Integer(r)) => match op {
                ComparisonOperator::Eq => l == r,
                ComparisonOperator::Ne => l != r,
                ComparisonOperator::Lt => l < r,
                ComparisonOperator::Gt => l > r,
                ComparisonOperator::Lte => l <= r,
                ComparisonOperator::Gte => l >= r,
                _ => false,
            },
            
            // Tekstovna primerjava
            (Value::Varchar(l), Value::Varchar(r)) => match op {
                ComparisonOperator::Eq => l == r,
                ComparisonOperator::Ne => l != r,
                ComparisonOperator::Lt => l < r,
                ComparisonOperator::Gt => l > r,
                ComparisonOperator::Lte => l <= r,
                ComparisonOperator::Gte => l >= r,
                _ => false,
            },
            
            // Logična primerjava
            (Value::Boolean(l), Value::Boolean(r)) => match op {
                ComparisonOperator::Eq => l == r,
                ComparisonOperator::Ne => l != r,
                _ => false,
            },
            _ => false,
        }
    }

    /// Pomožna funkcija za varno primerjavo floatov z toleranco (epsilon)
    fn compare_floats(&self, l: f32, r: f32, op: &ComparisonOperator) -> bool {
        let epsilon = 1e-6; // Natančnost za f32 (za f64 bi uporabil 1e-9)
        match op {
            ComparisonOperator::Eq => (l - r).abs() < epsilon,
            ComparisonOperator::Ne => (l - r).abs() >= epsilon,
            ComparisonOperator::Lt => l < r,
            ComparisonOperator::Gt => l > r,
            ComparisonOperator::Lte => l <= r,
            ComparisonOperator::Gte => l >= r,
            _ => false,
        }
    }

    /// main filter function
    fn evaluate(&self, tuple: &HeapTuple, desc: &TupleDescriptor) -> bool {
        let values = desc.unpack_from_tuple(tuple);

        match &self.expression {
            Expression::ComparisonOp(left, op, right) => {
                let left_val = self.eval_expr(left, &values, desc);
                let right_val = self.eval_expr(right, &values, desc);
                self.eval_comparison(&left_val, op, &right_val)
            }
            Expression::BinaryOp(_left, _op, _right) => {
                let res = self.eval_expr(&self.expression, &values, desc); // handles composed expressions recursivelly
                match res {
                    Value::Boolean(b) => b,
                    _ => false,
                }
            }
            Expression::Literal(SQLValue::Boolean(b)) => *b, // WHERE trully
            _ => false, // TODO: Could throw error
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
// 3. PROJECTION EXECUTOR
// =========================================================================
pub struct ProjectionExecutor {
    child: Box<dyn Executor>,
    project_columns: Vec<String>, // selected column names
    output_schema: Arc<TupleDescriptor>,
}

impl ProjectionExecutor {
    pub fn new(child: Box<dyn Executor>, project_columns: Vec<String>) -> Self {
        let child_schema = child.get_tuple_desc();
        let mut out_columns = Vec::new();
        for col_name in &project_columns { // generate filtered schema (discard unselected columns)
            if let Some(col) = child_schema.columns.iter().find(|c| &c.name == col_name) {
                out_columns.push(col.clone());
            }
        }
        let output_schema = Arc::new(TupleDescriptor::new(out_columns));
        Self {
            child,
            project_columns,
            output_schema,
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
            for col_name in &self.project_columns { // filter selected columns
                if let Some(idx) = child_desc.columns.iter().position(|c| &c.name == col_name) {
                    projected_values.push(all_values[idx].clone());
                }
            }
            Some(self.output_schema.pack(projected_values)) // pack with new schema
        } else {
            None
        }
    }
    fn get_tuple_desc(&self) -> Arc<TupleDescriptor> {
        self.output_schema.clone()
    }
}

// =========================================================================
// 4. LIMIT EXECUTOR
// =========================================================================
pub struct LimitExecutor {
    child: Box<dyn Executor>,
    limit: usize,
    cursor: usize, // number of returned tuples so far
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
// 4b. SORT EXECUTOR
// =========================================================================
pub struct SortExecutor {
    child: Box<dyn Executor>,
    order_by: OrderBy, 
    sorted_tuples: Vec<HeapTuple>,
    cursor: usize,
}

impl SortExecutor {
    pub fn new(child: Box<dyn Executor>, order_by: OrderBy) -> Self {
        Self {
            child,
            order_by,
            sorted_tuples: Vec::new(),
            cursor: 0,
        }
    }
    fn compare_values(a: &Value, b: &Value) -> std::cmp::Ordering {
        match (a, b) { // TODO: Value should implement this
            (Value::Integer(va), Value::Integer(vb)) => va.cmp(vb),
            (Value::Varchar(va), Value::Varchar(vb)) => va.cmp(vb),
            (Value::Boolean(va), Value::Boolean(vb)) => va.cmp(vb),
            (Value::Timestamp(va), Value::Timestamp(vb)) => va.cmp(vb),
            (Value::Float(va), Value::Float(vb)) => {
                va.partial_cmp(vb).unwrap_or(Ordering::Equal)
            }
            (Value::Double(va), Value::Double(vb)) => {
                va.partial_cmp(vb).unwrap_or(Ordering::Equal)
            }
            (Value::Numeric(va), Value::Numeric(vb)) => va.cmp(vb),

            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => Ordering::Greater,
            (_, Value::Null) => Ordering::Less,
            
            (other_a, other_b) => other_a.as_str().cmp(&other_b.as_str()),
        }
    }
}

impl Executor for SortExecutor {
    fn init(&mut self) {
        self.child.init();
        self.sorted_tuples.clear();
        self.cursor = 0;
        let desc = self.child.get_tuple_desc();
        
        // TODO: Is this slow?
        let mut sort_keys = Vec::new(); // Shranili bomo (index_stolpca, is_descending)
        for field in &self.order_by.fields {
            if let Some(idx) = desc.columns.iter().position(|c| c.name == field.column) {
                sort_keys.push((idx, field.descending));
            }
        }

        // Vse vrstice naberemo v spomin
        while let Some(tuple) = self.child.next() {
            self.sorted_tuples.push(tuple);
        }

        // 2. Glavna logika za večnivojsko sortiranje
        self.sorted_tuples.sort_by(|a, b| {
            let row_a = desc.unpack_from_tuple(a);
            let row_b = desc.unpack_from_tuple(b);

            // Pregledamo vse stolpce po vrstnem redu pomembnosti
            for &(col_idx, is_descending) in &sort_keys {
                let val_a = &row_a[col_idx];
                let val_b = &row_b[col_idx];
                
                let mut ordering = Self::compare_values(val_a, val_b);
                
                if is_descending {
                    ordering = ordering.reverse();
                }

                // Če sta vrednosti različni, imamo zmagovalca in končamo primerjavo za to vrstico!
                if ordering != Ordering::Equal {
                    return ordering;
                }
                // Če sta enaki, zanka nadaljuje na naslednji stolpec (npr. illiteracy)
            }

            Ordering::Equal // Če so vsi stolpci enaki
        });
    }
    fn next(&mut self) -> Option<HeapTuple> {
        if self.cursor < self.sorted_tuples.len() {
            let tuple = self.sorted_tuples[self.cursor].clone();
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
// 5. EXECUTOR ENGINE
// =========================================================================

pub struct ExecutionEngine {
    bpm: Arc<BufferPoolManager>,
    sm: Arc<crate::storage::manager::StorageManager>,
    pub tm: Arc<TransactionManager>,
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

    /// Creates an execution plan and executes the query.
    pub fn execute_statement(&self, statement: SQLStatement) 
        -> Result<(Vec<Vec<Value>>, Arc<TupleDescriptor>), String> {
        match statement {
            // =========================================================================
            // 1. SELECT
            // =========================================================================
            SQLStatement::Select {
                columns,
                table_name,
                where_clause,
                order_by,
                limit,
            } => {
                let table_oid = self.cm.get_table_oid(&table_name)
                    .ok_or_else(|| format!("Table '{}' not found!", table_name))?;

                let schema = Arc::new(self.cm.get_schema(table_oid)); // get table schema
                let table_handle = self.sm.get_table(table_oid); // get table access handle

                // Each next plan wraps the previous one, so we build it from the bottom up.
                let mut plan: Box<dyn Executor> = Box::new(SeqScanExecutor::new(
                    self.bpm.clone(),
                    self.tm.clone(),
                    table_handle,
                    schema,
                ));
                if let Some(where_struct) = where_clause {
                    plan = Box::new(FilterExecutor::new(plan, where_struct.condition));
                }
                if let Some(sort_col_name) = order_by {
                    plan = Box::new(SortExecutor::new(plan, sort_col_name));
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
                        plan = Box::new(ProjectionExecutor::new(plan, project_names));
                    }
                }
                if let Some(lim_val) = limit {
                    plan = Box::new(LimitExecutor::new(plan, lim_val as usize));
                }
                plan.init();
                let output_desc = plan.get_tuple_desc();
                let mut results = Vec::new();

                while let Some(tuple) = plan.next() {
                    let unpacked = output_desc.unpack_from_tuple(&tuple);
                    results.push(unpacked);
                }

                Ok((results, output_desc))
            }

        // =========================================================================
        // 2. INSERT
        // =========================================================================

        SQLStatement::Insert {
            table_name,
            columns: _, 
            values,
        } => {
            let table_oid = self.cm.get_table_oid(&table_name).
                ok_or_else(|| format!("Table '{}' not found!", table_name))?;
            let schema = Arc::new(self.cm.get_schema(table_oid));
            let mut inserted_count = 0; // for reporting only
            for row in values {
                let mut row_values = Vec::new();
                for sql_val in row {
                    let val = match sql_val { // TODO: this repeats often, write helper function
                        SQLValue::Integer(num) => Value::Integer(num as i32),
                        SQLValue::String(s) => Value::Varchar(s.clone()),
                        SQLValue::Boolean(b) => Value::Boolean(b),
                        SQLValue::Float(f) => Value::Float(f as f32),
                        SQLValue::Null => Value::Null,
                    };
                    row_values.push(val);
                }
                let mut tuple = schema.pack(row_values);
                HeapAccess::insert(self.sm.clone(), table_oid, &mut tuple);
                inserted_count += 1;
            }
            // Report back with number of inserted rows
            let out_desc = Arc::new(TupleDescriptor::new(vec![
                tuple::desc::Column {
                    name: "INSERTED".to_string(),
                    data_type: DataType::Integer, 
                }
            ]));
            Ok((vec![vec![Value::Integer(inserted_count)]], out_desc))
        }

        // =========================================================================
        // 3. CREATE TABLE
        // =========================================================================

        SQLStatement::CreateTable {
            name,
            columns,
            if_not_exists,
        } => {
            let existing_oid = self.cm.get_table_oid(&name);
            if existing_oid.is_some() {
                if if_not_exists {
                    // Report back that table already exists
                    let out_desc = Arc::new(TupleDescriptor::new(vec![
                        tuple::desc::Column {
                            name: "CREATE_TABLE".to_string(),
                            data_type: DataType::Varchar(264),
                        }
                    ]));
                    return Ok((vec![vec![Value::Varchar(format!("Table {} already exists, skipping.", name))]], out_desc));
                } else {
                    return Err(format!("Table '{}' already exists!", name));
                }
            }
            // This could also be a helper function
            let mut catalog_columns = Vec::new();
            for col_def in columns {
                let system_type = match col_def.data_type {
                    DataTypeDef::Int | DataTypeDef::Integer => DataType::Integer,
                    DataTypeDef::Varchar(size) => DataType::Varchar(size.unwrap_or(255) as u16),
                    DataTypeDef::Text => DataType::Varchar(1000), // TODO: Text type not really implemented, critical.
                    DataTypeDef::Boolean => DataType::Boolean,
                    DataTypeDef::Float | DataTypeDef::Double => DataType::Float,
                    // Dodaj poljubne preostale tipe, ki jih tvoj DataType podpira
                    _ => DataType::Integer, 
                };
                catalog_columns.push(tuple::desc::Column {
                    name: col_def.name.clone(),
                    data_type: system_type,
                });
            }
            let new_table_schema = TupleDescriptor::new(catalog_columns); // create table schema
            let new_oid = self.cm.create_table(&name, 0, &new_table_schema);
            // Report back with a message:
            let out_desc = Arc::new(TupleDescriptor::new(vec![
                tuple::desc::Column {
                    name: "CREATE_TABLE".to_string(),
                    data_type: DataType::Varchar(264), 
                }
            ]));
            Ok((
                vec![vec![Value::Varchar(format!("Table {} created successfully with OID {}.", name, new_oid))]], 
                out_desc
            ))
        }

        // =========================================================================
        // 4. DROP TABLE
        // =========================================================================

        SQLStatement::DropTable {
            name,
            if_exists,
        } => {
            let success = self.cm.drop_table(&name);

            let message = if success {
                format!("Table {} dropped successfully.", name)
            } else {
                if if_exists {
                    format!("Table {} does not exist, skipping.", name)
                } else {
                    return Err(format!("Can not drop non existent table '{}'!", name));
                }
            };
            let out_desc = Arc::new(TupleDescriptor::new(vec![
                tuple::desc::Column {
                    name: "DROP_TABLE".to_string(),
                    data_type: DataType::Varchar(264),
                }
            ]));
            Ok((vec![vec![Value::Varchar(message)]], out_desc))
        }

        // =========================================================================
        // 5. DELETE FROM
        // =========================================================================

        SQLStatement::Delete {
            table_name,
            where_clause,
        } => {
            let table_oid = self.cm.get_table_oid(&table_name).
                ok_or_else(|| format!("Table '{}' not found!", table_name))?;
            let schema = Arc::new(self.cm.get_schema(table_oid));
            let table_handle = self.sm.get_table(table_oid);

            // build a plan first
            let mut plan: Box<dyn Executor> = Box::new(SeqScanExecutor::new(
                self.bpm.clone(),
                self.tm.clone(),
                table_handle,
                schema.clone(),
            ));
            if let Some(where_struct) = where_clause {
                plan = Box::new(FilterExecutor::new(plan, where_struct.condition));
            }
            plan.init();
            let mut rids_to_delete = Vec::new();
            while let Some(tuple) = plan.next() { // gather all RowIds of tuples to delete
                let page_id = tuple.header.t_ctid_page;
                let slot_num = tuple.header.t_ctid_slot;
                let rid = crate::common::types::RowId { page_id, slot_num };
                rids_to_delete.push(rid);
            }
            drop(plan); // HeapScan drops its pin on the page

            let mut deleted_count = 0; // only for reporting
            for rid in rids_to_delete {
                let success = HeapAccess::delete(
                    self.sm.clone(),
                    table_oid,
                    rid
                );
                if success {
                    deleted_count += 1;
                }
            }
            let out_desc = Arc::new(TupleDescriptor::new(vec![
                tuple::desc::Column {
                    name: "DELETED".to_string(),
                    data_type: DataType::Integer,
                }
            ]));
            Ok((vec![vec![Value::Integer(deleted_count)]], out_desc))
        }

        // =========================================================================
        // 6. UPDATE
        // =========================================================================

        SQLStatement::Update { // similar idea to delete
            table_name,
            assignments,
            where_clause,
        } => {
            let table_oid = self.cm.get_table_oid(&table_name).
                ok_or_else(|| format!("Table '{}' not found!", table_name))?;
            let schema = Arc::new(self.cm.get_schema(table_oid));
            let table_handle = self.sm.get_table(table_oid);
            let mut plan: Box<dyn Executor> = Box::new(SeqScanExecutor::new(
                self.bpm.clone(),
                self.tm.clone(),
                table_handle,
                schema.clone(),
            ));
            if let Some(where_struct) = where_clause {
                plan = Box::new(FilterExecutor::new(plan, where_struct.condition));
            }
            plan.init();

            let mut targets = Vec::new();
            while let Some(tuple) = plan.next() {
                let rid = crate::common::types::RowId { 
                    page_id: tuple.header.t_ctid_page, 
                    slot_num: tuple.header.t_ctid_slot 
                };
                targets.push((rid, tuple));
            }
            drop(plan);

            let mut updated_count = 0;
            for (rid, old_tuple) in targets {
                let mut current_values = schema.unpack_from_tuple(&old_tuple);
                for (col_name, expr) in &assignments {
                    if let Some(col_idx) = schema.columns.iter().position(|c| c.name == *col_name) {
                        if let Expression::Literal(sql_val) = expr {
                            current_values[col_idx] = match sql_val {
                                SQLValue::Integer(i) => Value::Integer(*i as i32),
                                SQLValue::String(s) => Value::Varchar(s.clone()),
                                SQLValue::Boolean(b) => Value::Boolean(*b),
                                SQLValue::Float(f) => Value::Float(*f as f32),
                                SQLValue::Null => Value::Null,
                            };
                        }
                    }
                }
                let mut new_tuple = schema.pack(current_values);
                HeapAccess::update(
                    self.sm.clone(),
                    table_oid,
                    rid,
                    &mut new_tuple
                );
                updated_count += 1;
            }

            let out_desc = Arc::new(TupleDescriptor::new(vec![
                tuple::desc::Column {
                    name: "UPDATED".to_string(),
                    data_type: DataType::Integer,
                }
            ]));
            Ok((vec![vec![Value::Integer(updated_count)]], out_desc))
        }

        // =========================================================================
        // UNSUPORTED
        // =========================================================================
        _ => {
            let out_desc = Arc::new(TupleDescriptor::new(Vec::new()));
            Ok((vec![vec![Value::Varchar("Unsupported statement".to_string())]], out_desc))
        }
    }
}
}

impl ExecutionEngine {
    pub fn run_script_in_transaction(&self, code: &str, xid: TransactionId) -> Result<(), String> {
        let mut parser = SQLParser::new(code);
        let statements = parser.parse_script()?;
        for stmt in statements {
            self.execute_statement(stmt)?;
        }
        Ok(())
    }
}