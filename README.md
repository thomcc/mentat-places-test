# Mentat / Places test

I wanted to see how mentat would handle inserting all of the history places and visits from places.sqlite into a mentat database. This does that. The schema I used is pasted into the bottom of this file. It's based on the schema from [the mentat fixtures dir](https://github.com/mozilla/mentat/blob/master/fixtures/cities.schema), and what seemed relevant from [the datomic best practices guide](https://docs.datomic.com/cloud/best.html).

Usage: `cargo run --release path/to/your/places.sqlite`. Alternate usage `cargo run --release path/to/your/places.sqlite path/to/mentat_places.db` (you can pass path to the output db as a 2nd arg, it defaults to `mentat_places.db` in your cwd). It will show you progress, although the total is an approximation because it's 11PM on a Monday and this took longer than I expected.

(If you get an error, one of the `unwrap()`s, unchecked array accesses, or similar things in this code hit a case on your machine it didn't hit on mine. Sorry, this is very far from the most robust code I've ever written. Ping me in IRC, or file an issue if you hit this but would really like to give it a go for whatever reason)

This will produce a `mentat_places.db` file which is your mentat database. You then can open this from the mentat_cli using `cargo run --release -p mentat_cli -d path/to/mentat_places.db` from the mentat repository (remember to do `.timer on` to enable timing of queries and expressions in mentat's CLI).

The schema is in `places.schema`. You can modify it and the program will use the change without recompiling, but it may or may not work depending on what you change.

## Some notes:

### Inserting

Inserting is not fast. For me it took over 20 minutes(!) to insert around 50k visits, and by the end it was taking over 50ms to insert a single visit (it got slower as the DB filled up).

I would imagine that my code is partially to blame for this, I have no idea what the efficient APIs for inserting into mentat are, I used the ones that weren't obviously bad (e.g. I stayed away from the ones that required parsing strings).

Additionally, I'd imagine it's likely to speed up to some extent though, although I don't know how much. That said, 20min for 50k visits was *vastly* slower than I expected, and I'm glad I didn't run it on my full history (which, sadly, perished in the line of duty while writing this, when I accidentally deleted it instead of the `mentat_places.db` file).

Edit: I realized this too late to feel like incorporating it into this writeup in an intelligent fashion, but turning off `:db/fulltext` makes things a good amount faster here (5min instead of 20min).

### Database size

I didn't expect this, but the output mentat DB is somewhat huge. My input places DB is around 13MB (and it contains bookmarks too, although only a couple hundred, the lions share here is certainly the places data).

The output of my mentat DB is around 61MB (which also contains what probably amounts to a negligable number of bookmarks). It compresses about 57% under `lz4 -1`, and only slightly better for `-9`. `zstd -22` manages to reduce it to around 17MB, but it's still substantially larger than places.sqlite for reasons I don't understand.

Edit: I thought about it some and realized that maybe the usage of `:db/fulltext true` is responsible so I redid without that and found that performance was somewhat better (only around 5 minutes to build the DB), but the database file itself was 80MB!!. I don't have any idea as to why it would be this way.

### Queries

I don't really know how to write queries for this well. Note that this ended up getting compiled with rust beta since I hit some sort of hang in rustc when building mentat_cli as a release build on stable. 😐

I also only wrote two of these since I'm not at the point where I can write them without thinking a lot and referring to examples. And it's late.

---

Find the most recently visited 10 urls:

```edn
[:find [?urls ...]
 :order (desc ?when)
 :where [?visit :visit/date ?when]
        [?visit :visit/place ?place]
        [?place :place/url ?url]
 :limit 10]
```

Took around 200ms on my machine consistently.

The equivalent SQL query on places (`SELECT p.url FROM moz_places p JOIN moz_historyvisits v ON p.id = v.place_id ORDER BY v.visit_date DESC LIMIT 10`) took an average of 4ms over a few runs.

---

Find the dates of all visits to the facebook.com homepage. I got `47358661893743` out of my local places database as url_hash for `https://www.facebook.com/` to have a more accurate comparisons.

```edn
[:find ?visit_date
 :where [?place :place/url_hash 47358661893743]
        [?visit :visit/place ?place]
        [?visit :visit/date ?visit_date]]
```

For me this took around 50ms consistently (20 results). The equivalent version with url instead of url_hash took about 100ms.

The equivalent SQL queries on the places db (`SELECT v.visit_date FROM moz_historyvisits v JOIN moz_places p ON p.id = v.place_id WHERE p.url_hash = 47358661893743` and the similar one that uses `p.url` and `'https://www.facebook.com/'` instead) ran in 4ms and 7ms respectively.

---

These times are much worse than with sqlite directly, but they might not be so bad that they're unacceptable. Although, this may only seem this way due to my computer being fast and the number of items in my history being small.

### Conclusion

I don't have one, really.

Overall, the performance is far from great (queries seem to be 1-2 orders of magnitude slower than direct sqlite, and insertion varies a lot based on the schema), but I don't know how much optimizing has gone into what SQL is generated (certainly there's a lot of reasonably obvious micro-optimizations in the mentat source, but I don't think these are why it's slow). I also don't really know how to use features like materialized views which might help some here.

It isn't so bad as to seem impossible to use for my machine, so long as FTS is off (or we're only querying).

That said, this is a fast computer, and 50k visits over 25k places is a very small number in terms of someone's history, and it would be interesting to see it done with a more representative sample. (Sadly, my longer history was a casualty of a typo I made during this experiment, and don't really feel like getting a rust development environment set up on my personal computer).

All that said, the real conclusion here could be that I'm writing inefficient queries and calling mentat's APIs in slow ways, I really don't know.

## License

This code is cc0 / public domain.

