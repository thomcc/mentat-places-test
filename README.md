# Mentat / Places test

I wanted to see how mentat would handle inserting all of the history places and visits from places.sqlite into a mentat database. This does that. The schema I used is [here](https://github.com/thomcc/mentat-places-test/blob/master/places.schema). It's based on the schema from [the mentat fixtures dir](https://github.com/mozilla/mentat/blob/master/fixtures/cities.schema), and what seemed relevant from [the datomic best practices guide](https://docs.datomic.com/cloud/best.html).

Usage: `cargo run --release path/to/your/places.sqlite`. Alternate usage `cargo run --release path/to/your/places.sqlite path/to/mentat_places.db` (you can pass path to the output db as a 2nd arg, it defaults to `mentat_places.db` in your cwd). It will show you progress, although the total is an approximation because it's 11PM on a Monday and this took longer than I expected.

(If you get an error, one of the `unwrap()`s, unchecked array accesses, or similar things in this code hit a case on your machine it didn't hit on mine. Sorry, this is very far from the most robust code I've ever written. Ping me in IRC, or file an issue if you hit this but would really like to give it a go for whatever reason)

This will produce a `mentat_places.db` file which is your mentat database. You then can open this from the mentat_cli using `cargo run --release -p mentat_cli -d path/to/mentat_places.db` from the mentat repository (remember to do run `.timer on` to enable timing of queries and expressions in mentat's CLI -- if you want to time SQLite running on your places.sqlite as a comparison, `.timer on` works for it as well).

The schema is in `places.schema`. You can modify it and the program will use the change without recompiling, but it may or may not work depending on what you change.

## License

This code is cc0 / public domain.

