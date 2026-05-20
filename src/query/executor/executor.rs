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
use crate::access::heap::access::HeapAccess;
use crate::catalog::types::DataType;
use crate::access::tuple;

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
            Expression::BinaryOp(_left, _op, _right) => {
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
// 4b. SORT EXECUTOR (Urejanje vrstic - ORDER BY z ASC/DESC)
// =========================================================================
pub struct SortExecutor {
    child: Box<dyn Executor>,
    // Shranimo celotno strukturo, da vemo ime stolpca in smer
    order_by: OrderBy, 
    sorted_tuples: Vec<HeapTuple>,
    cursor: usize,
}

impl SortExecutor {
    // Sprejmemo celoten OrderBy objekt
    pub fn new(child: Box<dyn Executor>, order_by: OrderBy) -> Self {
        Self {
            child,
            order_by,
            sorted_tuples: Vec::new(),
            cursor: 0,
        }
    }

    fn compare_values(a: &Value, b: &Value) -> std::cmp::Ordering {
        use crate::catalog::types::Value;
        use std::cmp::Ordering;

        match (a, b) {
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
        
        // Poiščemo indeks preko self.order_by.column
        let sort_col_idx = match desc.columns.iter().position(|c| c.name == self.order_by.column) {
            Some(idx) => idx,
            None => {
                while let Some(tuple) = self.child.next() {
                    self.sorted_tuples.push(tuple);
                }
                return;
            }
        };

        while let Some(tuple) = self.child.next() {
            self.sorted_tuples.push(tuple);
        }

        self.sorted_tuples.sort_by(|a, b| {
            let val_a = &desc.unpack_from_tuple(a)[sort_col_idx];
            let val_b = &desc.unpack_from_tuple(b)[sort_col_idx];
            
            let ordering = Self::compare_values(val_a, val_b);

            // Če je označeno 'descending', rezultat primerjave enostavno obrnemo!
            if self.order_by.descending {
                ordering.reverse()
            } else {
                ordering
            }
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

    /// Sprejme parsano AST poizvedbo in zgradi drevo operatorjev ter jo izvede.
    /// Vrne par: (Rezultati v obliki matrike Value, Shema izhodnih podatkov)
    pub fn execute_statement(&self, statement: SQLStatement, xid: TransactionId) -> (Vec<Vec<Value>>, Arc<TupleDescriptor>) {
        match statement {
           // =========================================================================
            // 1. SELECT UKAZ (Z vgrajeno podporo za ORDER BY preko SortExecutorja)
            // =========================================================================
            SQLStatement::Select {
                columns,
                table_name,
                where_clause,
                order_by, // Spremenjeno iz order_by: _
                limit,
            } => {
                let table_oid = self.cm.get_table_oid(&table_name).expect("Tabela ne obstaja v katalogu!");
                let schema = Arc::new(self.cm.get_schema(table_oid));
                let table_handle = self.sm.get_table(table_oid);

                // 1. Osnovni korak: SeqScan čez celo tabelo
                let mut plan: Box<dyn Executor> = Box::new(SeqScanExecutor::new(
                    self.bpm.clone(),
                    self.tm.clone(),
                    table_handle,
                    schema,
                ));

                // 2. Filtriranje vrstic (WHERE pogoj)
                if let Some(where_struct) = where_clause {
                    plan = Box::new(FilterExecutor::new(plan, where_struct.condition));
                }

                // 3. Urejanje vrstic (ORDER BY)
                // Tukaj vstaviva najin novi SortExecutor pred projekcijo, 
                // da ima dostop do vseh stolpcev tabele.
                if let Some(sort_col_name) = order_by {
                    plan = Box::new(SortExecutor::new(plan, sort_col_name));
                }

                // 4. Projekcija (Izbira specifičnih stolpcev)
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

                // 5. Omejitev števila vrstic (LIMIT)
                if let Some(lim_val) = limit {
                    plan = Box::new(LimitExecutor::new(plan, lim_val as usize));
                }

                // 6. Izvedba poizvedbe in pakiranje rezultatov
                plan.init();
                let output_desc = plan.get_tuple_desc();
                let mut results = Vec::new();

                while let Some(tuple) = plan.next() {
                    let unpacked = output_desc.unpack_from_tuple(&tuple);
                    results.push(unpacked);
                }

                (results, output_desc)
            }

        // =========================================================================
        // 2. INSERT UKAZ (Popravljen z uporabo tvoje strukture Column)
        // =========================================================================
        SQLStatement::Insert {
            table_name,
            columns: _, 
            values,
        } => {
            let table_oid = self.cm.get_table_oid(&table_name).expect("Tabela ne obstaja!");
            let schema = Arc::new(self.cm.get_schema(table_oid));
            
            let mut inserted_count = 0;

            for row in values {
                let mut row_values = Vec::new();
                
                for sql_val in row {
                    let val = match sql_val {
                        SQLValue::Integer(num) => Value::Integer(num as i32),
                        SQLValue::String(s) => Value::Varchar(s.clone()),
                        SQLValue::Boolean(b) => Value::Boolean(b),
                        SQLValue::Float(f) => Value::Float(f as f32),
                        SQLValue::Null => Value::Null,
                    };
                    row_values.push(val);
                }

                let mut tuple = schema.pack(row_values, xid);

                HeapAccess::insert(self.sm.clone(), xid, table_oid, &mut tuple);
                inserted_count += 1;
            }

            // --- TOLE JE POPRAVEK ---
            // Neposredno uporabimo tvojo strukturo `Column`
            let out_desc = Arc::new(TupleDescriptor::new(vec![
                tuple::desc::Column {
                    name: "INSERTED".to_string(),
                    data_type: DataType::Integer, 
                }
            ]));

            (vec![vec![Value::Integer(inserted_count)]], out_desc)
        }

        // =========================================================================
        // 3. CREATE TABLE UKAZ (Zdaj zares ustvari tabelo in jo vpiše v katalog)
        // =========================================================================
        SQLStatement::CreateTable {
            name,
            columns,
            if_not_exists,
        } => {
            // Preverimo, če tabela že obstaja v katalogu
            let existing_oid = self.cm.get_table_oid(&name);
            
            if existing_oid.is_some() {
                if if_not_exists {
                    let out_desc = Arc::new(TupleDescriptor::new(vec![
                        tuple::desc::Column {
                            name: "CREATE_TABLE".to_string(),
                            data_type: DataType::Varchar(264),
                        }
                    ]));
                    return (vec![vec![Value::Varchar(format!("Table {} already exists, skipping.", name))]], out_desc);
                } else {
                    panic!("Tabela {} že obstaja!", name);
                }
            }

            // Prevedemo stolpce iz Parser definicije (ColumnDef) v prave katalog stolpce (Column)
            let mut catalog_columns = Vec::new();
            for col_def in columns {
                let system_type = match col_def.data_type {
                    DataTypeDef::Int | DataTypeDef::Integer => DataType::Integer,
                    DataTypeDef::Varchar(size) => DataType::Varchar(size.unwrap_or(255) as u16),
                    DataTypeDef::Text => DataType::Varchar(1000), // Če nimaš posebnega Text tipa
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

            // Ustvarimo TupleDescriptor za novo tabelo
            let new_table_schema = TupleDescriptor::new(catalog_columns);

            // Pokličemo CatalogManager, da zares ustvari datoteko in vpiše metapodatke v rg_class/rg_attribute
            // (Predvidevam, da imaš v ExecutionEngine dostop do self.cm kot CatalogManager)
            let new_oid = self.cm.create_table(xid, &name, 0, &new_table_schema);

            // Pripravimo izhodni rezultat za spletni vmesnik
            let out_desc = Arc::new(TupleDescriptor::new(vec![
                tuple::desc::Column {
                    name: "CREATE_TABLE".to_string(),
                    data_type: DataType::Varchar(264), 
                }
            ]));

            (
                vec![vec![Value::Varchar(format!("Table {} created successfully with OID {}.", name, new_oid))]], 
                out_desc
            )
        }

        // =========================================================================
        // 4. DROP TABLE UKAZ (Izbriše datoteko in spuca sistemske kataloge)
        // =========================================================================
        SQLStatement::DropTable {
            name,
            if_exists,
        } => {
            let success = self.cm.drop_table(xid, &name);

            let message = if success {
                format!("Table {} dropped successfully.", name)
            } else {
                if if_exists {
                    format!("Table {} does not exist, skipping.", name)
                } else {
                    panic!("Tabela {} ne obstaja za izbris!", name);
                }
            };

            let out_desc = Arc::new(TupleDescriptor::new(vec![
                tuple::desc::Column {
                    name: "DROP_TABLE".to_string(),
                    data_type: DataType::Varchar(264),
                }
            ]));

            (vec![vec![Value::Varchar(message)]], out_desc)
        }

// =========================================================================
        // 5. DELETE FROM UKAZ (Uporaba t_ctid in obstoječega HeapAccess mehanizma)
        // =========================================================================
        SQLStatement::Delete {
            table_name,
            where_clause,
        } => {
            let table_oid = self.cm.get_table_oid(&table_name).expect("Tabela ne obstaja v katalogu!");
            let schema = Arc::new(self.cm.get_schema(table_oid));
            let table_handle = self.sm.get_table(table_oid);

            // 1. Zgradimo standardni izvajalni načrt za iskanje vrstic
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

            // Sestavimo strukturo RowId, ki jo potrebuje HeapAccess::delete.
            // (Predvidevam, da imaš RowId definiran kot struct { page_id: u32, slot_num: u16 })
            let mut rids_to_delete = Vec::new();

            // 2. Faza: Zberemo vse RowId-je ustreznih vrstic
            while let Some(tuple) = plan.next() {
                // Iz glave vrstice izluščimo ctid koordinate, ki jih je tvoj HeapScan nastavil
                let page_id = tuple.header.t_ctid_page;
                let slot_num = tuple.header.t_ctid_slot;
                
                // Ustvarimo RowId objekt za tvoj HeapAccess::delete
                let rid = crate::common::types::RowId { page_id, slot_num };
                rids_to_delete.push(rid);
            }

            // Eksplicitno uničimo (drop) plan, da HeapScan sprosti aktivne frame (unpin) in zaklepe
            drop(plan);

            let mut deleted_count = 0;

            // 3. Faza: Dejanski MVCC izbris preko obstoječe HeapAccess funkcije
            for rid in rids_to_delete {
                let success = HeapAccess::delete(
                    self.sm.clone(),
                    xid,
                    table_oid,
                    rid
                );
                
                if success {
                    deleted_count += 1;
                }
            }

            // 4. Priprava izhodne sheme in rezultata
            let out_desc = Arc::new(TupleDescriptor::new(vec![
                tuple::desc::Column {
                    name: "DELETED".to_string(),
                    data_type: DataType::Integer,
                }
            ]));

            (vec![vec![Value::Integer(deleted_count)]], out_desc)
        }

// =========================================================================
        // 6. UPDATE UKAZ (MVCC: Delete + Insert preko HeapAccess::update)
        // =========================================================================
        SQLStatement::Update {
            table_name,
            assignments,
            where_clause,
        } => {
            let table_oid = self.cm.get_table_oid(&table_name).expect("Tabela ne obstaja!");
            let schema = Arc::new(self.cm.get_schema(table_oid));
            let table_handle = self.sm.get_table(table_oid);

            // 1. Poiščemo vrstice za posodobitev
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

            // 2. Posodobitev vsake vrstice
            for (rid, old_tuple) in targets {
                let mut current_values = schema.unpack_from_tuple(&old_tuple);

                // Uporabimo vrednosti iz assignmentov
                for (col_name, expr) in &assignments {
                    if let Some(col_idx) = schema.columns.iter().position(|c| c.name == *col_name) {
                        // Ker so v tvojem parserju zdaj izrazi, jih v najpreprostejši obliki 
                        // pretvorimo iz Literal vrednosti (kot smo delali pri Insert)
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

                // Ustvarimo nov tuple in ga posodobimo prek tvoje HeapAccess::update
                let mut new_tuple = schema.pack(current_values, xid);
                
                HeapAccess::update(
                    self.sm.clone(),
                    xid,
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

            (vec![vec![Value::Integer(updated_count)]], out_desc)
        }

        // =========================================================================
        // PRIVZETE VEJE (Popravljene na prazen TupleDescriptor)
        // =========================================================================
        _ => {
            let out_desc = Arc::new(TupleDescriptor::new(Vec::new()));
            (vec![vec![Value::Varchar("Unsupported statement".to_string())]], out_desc)
        }


    }
}
}