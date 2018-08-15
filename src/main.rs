#[macro_use]
extern crate mentat;
extern crate rusqlite;

use mentat::entity_builder::BuildTerms;
use std::{env, process};
use std::fs::{File};
use std::io::{Read, Write, self};

use rusqlite::{
    Connection,
    OpenFlags,
    Row,
};

use mentat::{
    HasSchema,
    Store,
    TypedValue,
    Entid,
};

struct PlaceEntry {
    pub id: i64,
    pub url: String,
    pub url_hash: i64,
    pub description: Option<String>,
    pub title: String,
    pub frecency: i64,
    pub visits: Vec<(i64, Entid)>,
}

impl PlaceEntry {
    pub fn add_to_store(&self, store: &mut Store) -> mentat::errors::Result<()> {

        let in_progress = store.begin_transaction()?;
        let (mut ip, place_ent_id) = {
            let mut builder = in_progress.builder().describe_tempid("place");
            builder.add(kw!(:place/url), TypedValue::typed_string(&self.url))?;
            builder.add(kw!(:place/url_hash), TypedValue::Long(self.url_hash))?;
            builder.add(kw!(:place/title), TypedValue::typed_string(&self.title))?;
            if let &Some(ref desc) = &self.description {
                builder.add(kw!(:place/description), TypedValue::typed_string(desc))?;
            }

            builder.add(kw!(:place/frecency), TypedValue::Long(self.frecency))?;

            let (ip, r) = builder.transact();
            (ip, *r.expect("builder transact place").tempids.get("place").expect("get")) // from above
        };

        assert!(self.visits.len() > 0);
        for &(microtime, visit_type) in &self.visits {
            let mut builder = ip.builder().describe_tempid("visit");
            builder.add(kw!(:visit/place), TypedValue::Ref(place_ent_id))?;
            builder.add(kw!(:visit/date), TypedValue::instant(microtime))?;
            builder.add(kw!(:visit/type), TypedValue::Ref(visit_type))?;
            let (prog, r) = builder.transact();
            r.expect("builder transact visit");
            ip = prog;
        }

        ip.commit()?;
        Ok(())
    }

    pub fn from_row(row: &Row, transition_type_entids: &[Entid]) -> PlaceEntry {
        let transition_type: i64 = row.get(7);
        PlaceEntry {
            id: row.get(0),
            url: row.get(1),
            url_hash: row.get(2),
            description: row.get(3),
            title: row.get::<i32, Option<String>>(4).unwrap_or("".into()),
            frecency: row.get(5),
            visits: vec![(row.get(6), transition_type_entids[
                (transition_type as usize).saturating_sub(1)])],
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

    let visit_types = &[
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

    let schema = read_file("./places.schema").expect(
        "Failed to read data from `places.schema` file");

    let in_db_path = args[1].clone();
    let out_db_path = args.get(2).cloned().unwrap_or("./mentat_places.db".into());
    let places = Connection::open_with_flags(in_db_path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let mut store = Store::open_empty(&out_db_path).unwrap();

    store.transact(&schema).expect("Failed to transact schema...");
    let type_to_ent_id = visit_types.iter().map(|kw|
        store.conn().current_schema().get_entid(kw).unwrap().0).collect::<Vec<_>>();

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
    while let Some(row_or_error) = rows.next() {
        let row = row_or_error.unwrap();
        let id: i64 = row.get(0);
        if current_place.id == id {
            let tty: i64 = row.get(7);
            current_place.visits.push((row.get(6), *type_to_ent_id.get((tty - 1) as usize).unwrap_or(&0)));
            continue;
        }
        if current_place.id >= 0 {
            current_place.add_to_store(&mut store).unwrap();
            print!("\rInserting {} / {} places (approx.)", so_far, place_count);
            io::stdout().flush().unwrap();
            so_far += 1;
        }
        current_place = PlaceEntry::from_row(&row, &type_to_ent_id);
    }
    if current_place.id >= 0 {
        current_place.add_to_store(&mut store).unwrap();
        print!("\rInserting {} / {} places (approx.)", so_far + 1, place_count);
    }
    println!("\nDone!")
}
