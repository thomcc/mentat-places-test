#[macro_use]
extern crate mentat;
extern crate rusqlite;

#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
extern crate env_logger;
extern crate clap;
extern crate find_places_db;
#[macro_use]
extern crate failure;
extern crate tempfile;

use std::fs;
use std::io::{Write, self};
use std::fmt::{Write as FmtWrite};
use std::path::Path;

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

// const MAX_TRANSACT_BUFFER_SIZE: usize = 1024 * 1024 * 1024;

#[derive(Debug, Clone)]
struct TransactBuilder {
    counter: u64,
    data: String,
    total_terms: u64,
    terms: u64,
    max_buffer_size: usize
}

impl TransactBuilder {
    #[inline]
    pub fn new_with_size(max_buffer_size: usize) -> Self {
        Self { counter: 0, data: "[\n".into(), terms: 0, total_terms: 0, max_buffer_size }
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
        self.data.len() >= self.max_buffer_size
    }

    #[inline]
    pub fn maybe_transact(&mut self, store: &mut Store) -> MentatResult<Option<mentat::TxReport>> {
        if self.should_finish() {
            Ok(self.transact(store)?)
        } else {
            Ok(None)
        }
    }

    #[inline]
    pub fn transact(&mut self, store: &mut Store) -> MentatResult<Option<mentat::TxReport>> {
        if self.terms != 0 {
            debug!("\nTransacting {} terms (total = {})", self.terms, self.total_terms);
            let res = store.transact(self.finish());
            if res.is_err() { error!("Error transacting:\n{}", self.data); }
            let report = res?;
            self.reset();
            Ok(Some(report))
        } else {
            Ok(None)
        }
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
    pub fn add(&self, builder: &mut TransactBuilder, store: &mut Store) -> Result<(), failure::Error> {
        let place_id = builder.next_tempid();
        builder.add_str(place_id, &*PLACE_URL, &self.url);
        builder.add_long(place_id, &*PLACE_URL_HASH, self.url_hash);
        builder.add_str(place_id, &*PLACE_TITLE, &self.title);
        if let Some(desc) = &self.description {
            builder.add_str(place_id, &*PLACE_DESCRIPTION, desc);
        }

        builder.add_long(place_id, &*PLACE_FRECENCY, self.frecency);

        assert!(self.visits.len() > 0);

        if builder.max_buffer_size == 0 {
            let report = builder.transact(store)?.unwrap();
            let place_eid = report.tempids.get(&format!("{}", place_id)).unwrap();
            // One transaction per visit.
            for (microtime, visit_type) in &self.visits {
                let visit_id = builder.next_tempid();
                builder.add_long(visit_id, &*VISIT_PLACE, *place_eid);
                builder.add_inst(visit_id, &*VISIT_DATE, *microtime);
                builder.add_kw(visit_id, &*VISIT_TYPE, visit_type);
                builder.transact(store)?;
            }
        } else {
            for (microtime, visit_type) in &self.visits {
                let visit_id = builder.next_tempid();
                builder.add_ref_to_tmpid(visit_id, &*VISIT_PLACE, place_id);
                builder.add_inst(visit_id, &*VISIT_DATE, *microtime);
                builder.add_kw(visit_id, &*VISIT_TYPE, visit_type);
            }
            builder.maybe_transact(store)?;
        }
        Ok(())
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

fn main() -> Result<(), failure::Error> {
    let matches = clap::App::new("mentat-places-test")
        .arg(clap::Arg::with_name("OUTPUT")
            .index(1)
            .help("Path where we should output the anonymized db (defaults to ./mentat_places.db)"))
        .arg(clap::Arg::with_name("PLACES")
            .index(2)
            .help("Path to places.sqlite. If not provided, we'll use the largest places.sqlite in your firefox profiles"))
        .arg(clap::Arg::with_name("v")
            .short("v")
            .multiple(true)
            .help("Sets the level of verbosity (pass up to 3 times for more verbosity -- e.g. -vvv enables trace logs)"))
        .arg(clap::Arg::with_name("force")
            .short("f")
            .long("force")
            .help("Overwrite OUTPUT if it already exists"))
        .arg(clap::Arg::with_name("realistic")
            .short("r")
            .long("realistic")
            .help("Insert everything with one transaction per visit. This is a lot slower, \
                   but is a more realistic workload. It produces databases that are ~40% larger (for me)."))
    .get_matches();

    env_logger::init_from_env(match matches.occurrences_of("v") {
        0 => env_logger::Env::default().filter_or("RUST_LOG", "warn"),
        1 => env_logger::Env::default().filter_or("RUST_LOG", "info"),
        2 => env_logger::Env::default().filter_or("RUST_LOG", "debug"),
        3 | _ => env_logger::Env::default().filter_or("RUST_LOG", "trace"),
    });


    let places_db = if let Some(places) = matches.value_of("PLACES") {
        let meta = fs::metadata(&places)?;
        find_places_db::PlacesLocation {
            profile_name: "".into(),
            path: fs::canonicalize(places)?,
            db_size: meta.len(),
        }
    } else {
        let mut dbs = find_places_db::get_all_places_dbs()?;
        if dbs.len() == 0 {
            error!("No dbs found!");
            bail!("No dbs found");
        }
        for p in &dbs {
            debug!("Found: profile {:?} with a {} places.sqlite", p.profile_name, p.friendly_db_size())
        }
        info!("Using profile {:?}", dbs[0].profile_name);
        dbs.into_iter().next().unwrap()
    };

    debug!("Copying places.sqlite to a temp file for reading");
    let temp_dir = tempfile::tempdir()?;
    let temp_places_path = temp_dir.path().join("places.sqlite");
    fs::copy(&places_db.path, &temp_places_path)?;
    let places = Connection::open_with_flags(&temp_places_path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;

    let out_db_path = matches.value_of("OUTPUT").unwrap_or_else(|| "./mentat_places.db".into());

    if Path::new(&out_db_path).exists() {
        if matches.is_present("force") {
            info!("Deleting previous `{}` because -f was passed", out_db_path);
            fs::remove_file(&out_db_path)?;
        } else {
            error!("{} already exists but `-f` argument was not provided", out_db_path);
            bail!("File already exists");
        }
    }

    let mut store = Store::open_empty(&out_db_path)?;
    debug!("Transacting initial schema");
    store.transact(include_str!("../places.schema"))?;

    let mut stmt = places.prepare("
        SELECT
            p.id,
            p.url,
            p.url_hash,
            p.description,
            p.title,
            p.frecency,
            v.visit_date,
            v.visit_type
        FROM moz_places p
        JOIN moz_historyvisits v
            ON p.id = v.place_id
        ORDER BY p.id
    ").unwrap();

    let (place_count, visit_count) = {
        let mut stmt = places.prepare("select count(*) from moz_places").unwrap();
        let mut rows = stmt.query(&[]).unwrap();
        let ps: i64 = rows.next().unwrap()?.get(0);

        let mut stmt = places.prepare("select count(*) from moz_historyvisits").unwrap();
        let mut rows = stmt.query(&[]).unwrap();
        let vs: i64 = rows.next().unwrap()?.get(0);
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

    let max_buffer_size = if matches.is_present("realistic") { 0 } else { 1024 * 1024 * 1024 * 1024 };
    let mut builder = TransactBuilder::new_with_size(max_buffer_size);

    let mut so_far = 0;
    let mut rows = stmt.query(&[])?;
    while let Some(row_or_error) = rows.next() {
        let row = row_or_error?;
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
            current_place.add(&mut builder, &mut store)?;
            // builder.maybe_transact(&mut store)?;
            print!("\rProcessing {} / {} places (approx.)", so_far, place_count);
            io::stdout().flush()?;
            so_far += 1;
        }
        current_place = PlaceEntry::from_row(&row);
    }

    if current_place.id >= 0 {
        current_place.add(&mut builder, &mut store)?;
        // builder.maybe_transact(&mut store)?;
        println!("\rProcessing {} / {} places (approx.)", so_far + 1, place_count);
    }
    builder.transact(&mut store)?;
    println!("Done!");
    Ok(())
}
