#[macro_use]
extern crate mentat;
extern crate rusqlite;

#[macro_use]
extern crate lazy_static;

use std::{env, process};
use std::fs::{self, File};
use std::io::{Read, Write, self};
use std::fmt::{Write as FmtWrite};

use rusqlite::{
    Connection,
    OpenFlags,
    Row,
};

use mentat::{
    Store,
    Keyword,
    errors::Result as MentatResult,
};

const MAX_TRANSACT_BUFFER_SIZE: usize = 1024 * 1024 * 1024;

#[derive(Debug, Clone)]
struct TransactBuilder {
    counter: u64,
    data: String,
    total_terms: u64,
    terms: u64,
}

impl TransactBuilder {
    #[inline]
    pub fn new() -> Self {
        Self { counter: 0, data: "[\n".into(), terms: 0, total_terms: 0 }
    }

    #[inline]
    pub fn next_tempid(&mut self) -> u64 {
        self.counter += 1;
        self.counter
    }

    #[inline]
    pub fn add_ref_to_tmpid(&mut self, tmpid: u64, attr: &Keyword, ref_tmpid: u64) {
        write!(self.data, " [:db/add \"{}\" {} \"{}\"]\n", tmpid, attr, ref_tmpid).unwrap();
        self.terms += 1;
        self.total_terms += 1;
    }

    #[inline]
    pub fn add_inst(&mut self, tmpid: u64, attr: &Keyword, micros: i64) {
        write!(self.data, " [:db/add \"{}\" {} #instmicros {}]\n", tmpid, attr, micros).unwrap();
        self.terms += 1;
        self.total_terms += 1;
    }

    #[inline]
    pub fn add_kw(&mut self, tmpid: u64, attr: &Keyword, val: &Keyword) {
        write!(self.data, " [:db/add \"{}\" {} {}]\n", tmpid, attr, val).unwrap();
        self.terms += 1;
        self.total_terms += 1;
    }

    #[inline]
    pub fn add_str(&mut self, tmpid: u64, attr: &Keyword, val: &str) {
        // {:?} escapes some chars EDN can't parse (e.g. \'...)
        let s = val.replace("\\", "\\\\").replace("\"", "\\\"");
        write!(self.data, " [:db/add \"{}\" {} \"{}\"]\n", tmpid, attr, s).unwrap();
        self.terms += 1;
        self.total_terms += 1;
    }

    #[inline]
    pub fn add_long(&mut self, tmpid: u64, attr: &Keyword, val: i64) {
        write!(self.data, " [:db/add \"{}\" {} {}]\n", tmpid, attr, val).unwrap();
        self.terms += 1;
        self.total_terms += 1;
    }

    #[inline]
    pub fn finish(&mut self) -> &str {
        self.data.push(']');
        &self.data
    }

    #[inline]
    pub fn reset(&mut self) {
        self.terms = 0;
        self.data.clear();
        self.data.push_str("[\n")
    }

    #[inline]
    pub fn should_finish(&self) -> bool {
        self.data.len() >= MAX_TRANSACT_BUFFER_SIZE
    }

    #[inline]
    pub fn maybe_transact(&mut self, store: &mut Store) -> MentatResult<()> {
        if self.should_finish() {
            self.transact(store)?;
        }
        Ok(())
    }

    #[inline]
    pub fn transact(&mut self, store: &mut Store) -> MentatResult<()> {
        if self.terms != 0 {
            println!("\nTransacting {} terms (total = {})", self.terms, self.total_terms);
            let res = store.transact(self.finish());
            if res.is_err() { println!("{}", self.data); }
            res?;
            self.reset();
        }
        Ok(())
    }
}

lazy_static! {
    static ref PLACE_URL: Keyword = kw!(:place/url);
    static ref PLACE_URL_HASH: Keyword = kw!(:place/url_hash);
    static ref PLACE_TITLE: Keyword = kw!(:place/title);
    static ref PLACE_DESCRIPTION: Keyword = kw!(:place/description);
    static ref PLACE_FRECENCY: Keyword = kw!(:place/frecency);
    static ref VISIT_PLACE: Keyword = kw!(:visit/place);
    static ref VISIT_DATE: Keyword = kw!(:visit/date);
    static ref VISIT_TYPE: Keyword = kw!(:visit/type);

    static ref VISIT_TYPES: Vec<Keyword> = vec![
        kw!(:visit.type/link),
        kw!(:visit.type/typed),
        kw!(:visit.type/bookmark),
        kw!(:visit.type/embed),
        kw!(:visit.type/redirect_permanent),
        kw!(:visit.type/redirect_temporary),
        kw!(:visit.type/download),
        kw!(:visit.type/framed_link),
        kw!(:visit.type/reload),
    ];
}

struct PlaceEntry {
    pub id: i64,
    pub url: String,
    pub url_hash: i64,
    pub description: Option<String>,
    pub title: String,
    pub frecency: i64,
    pub visits: Vec<(i64, &'static Keyword)>,
}

impl PlaceEntry {
    pub fn add(&self, builder: &mut TransactBuilder) {
        let place_id = builder.next_tempid();
        builder.add_str(place_id, &*PLACE_URL, &self.url);
        builder.add_long(place_id, &*PLACE_URL_HASH, self.url_hash);
        builder.add_str(place_id, &*PLACE_TITLE, &self.title);
        if let Some(desc) = &self.description {
            builder.add_str(place_id, &*PLACE_DESCRIPTION, desc);
        }

        builder.add_long(place_id, &*PLACE_FRECENCY, self.frecency);

        assert!(self.visits.len() > 0);
        for (microtime, visit_type) in &self.visits {
            let visit_id = builder.next_tempid();
            builder.add_ref_to_tmpid(visit_id, &*VISIT_PLACE, place_id);
            builder.add_inst(visit_id, &*VISIT_DATE, *microtime);
            builder.add_kw(visit_id, &*VISIT_TYPE, visit_type);
        }
    }

    pub fn from_row(row: &Row) -> PlaceEntry {
        let transition_type: i64 = row.get(7);
        PlaceEntry {
            id: row.get(0),
            url: row.get(1),
            url_hash: row.get(2),
            description: row.get(3),
            title: row.get::<i32, Option<String>>(4).unwrap_or("".into()),
            frecency: row.get(5),
            visits: vec![(row.get(6), &VISIT_TYPES[(transition_type as usize).saturating_sub(1)])],
        }
    }
}

fn read_file(path: &str) -> io::Result<String> {
    let mut file = File::open(path)?;
    let mut string = String::with_capacity((file.metadata()?.len() + 1) as usize);
    file.read_to_string(&mut string)?;
    Ok(string)
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        println!("usage: {} <path/to/places.sqlite> [out = ./mentat_places.db]", args[0]);
        process::exit(1);
    }


    let schema = read_file("./places.schema").expect(
        "Failed to read data from `places.schema` file");

    let in_db_path = args[1].clone();
    let out_db_path = args.get(2).cloned().unwrap_or("./mentat_places.db".into());
    let places = Connection::open_with_flags(in_db_path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    fs::remove_file(&out_db_path).unwrap();
    let mut store = Store::open_empty(&out_db_path).unwrap();

    store.transact(&schema).expect("Failed to transact schema...");
    // let type_to_ent_id = visit_types.iter().map(|kw|
        // store.conn().current_schema().get_entid(kw).unwrap().0).collect::<Vec<_>>();

    let mut stmt = places.prepare("
        SELECT
            p.id, p.url, p.url_hash, p.description, p.title, p.frecency,
            v.visit_date, v.visit_type
        FROM moz_places p
        JOIN moz_historyvisits v
            ON p.id = v.place_id
        ORDER BY p.id
    ").unwrap();

    let (place_count, visit_count) = {
        let mut stmt = places.prepare("select count(*) from moz_places").unwrap();
        let mut rows = stmt.query(&[]).unwrap();
        let ps: i64 = rows.next().unwrap().unwrap().get(0);

        let mut stmt = places.prepare("select count(*) from moz_historyvisits").unwrap();
        let mut rows = stmt.query(&[]).unwrap();
        let vs: i64 = rows.next().unwrap().unwrap().get(0);
        (ps, vs)
    };

    println!("Querying {} places ({} visits)", place_count, visit_count);

    let mut current_place = PlaceEntry {
        id: -1,
        url: "".into(),
        url_hash: 0,
        description: None,
        title: "".into(),
        frecency: 0,
        visits: vec![],
    };

    let mut so_far = 0;
    let mut rows = stmt.query(&[]).unwrap();
    let mut builder = TransactBuilder::new();

    while let Some(row_or_error) = rows.next() {
        let row = row_or_error.unwrap();
        let id: i64 = row.get(0);
        if current_place.id == id {
            let tty: i64 = row.get(7);
            current_place.visits.push((
                row.get(6),
                &VISIT_TYPES.get((tty.max(0) as usize).saturating_sub(1))
                    .unwrap_or_else(|| &VISIT_TYPES[0])
            ));
            continue;
        }

        if current_place.id >= 0 {
            current_place.add(&mut builder);
            builder.maybe_transact(&mut store).unwrap();
            print!("\rProcessing {} / {} places (approx.)", so_far, place_count);
            io::stdout().flush().unwrap();
            so_far += 1;
        }
        current_place = PlaceEntry::from_row(&row);
    }

    if current_place.id >= 0 {
        current_place.add(&mut builder);
        builder.maybe_transact(&mut store).unwrap();
        println!("\rProcessing {} / {} places (approx.)", so_far + 1, place_count);
    }
    builder.transact(&mut store).unwrap();
    println!("Done!");
}
