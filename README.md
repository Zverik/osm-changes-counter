# OpenStreetMap Changes Counter

These scripts were made for [this competition](http://osm-competition.tilda.ws/).
We count number of modified crossings and road segments per user per `place=city`.
And with that we rank them to determine who did the most work.

## Usage

You will need a PostgreSQL database. PostGIS is not needed. And Python 3.7+.

For filtering, you would need two files. One with tags: see `konkurs_tags.lst`
for an example. Each line has two or three space-delimited parts: object type
(`n` and `w` are supported), section name (can be repeated in multiple lines),
and a key or a tag.

If you need cities other than russian 100k+ cities, import your area to PostgreSQL
with [osm2pgsql](https://www.osm2pgsql.org/) and
[style for places](https://github.com/Zverik/city-mapping-stats/blob/main/scripts/highways-and-places.style),
and run `prepare_cities.sh`.

### Usage with osmChange files

So you've got an OSM extract (user names inside are optional). First, find the relevant
hourly replication sequence number. Go [here](https://planet.openstreetmap.org/replication/hour/000/)
and find an `osc.gz` file that's been created right after the last timestamp in the extract.

Now run `init_osc.sh` with five arguments:

* database name
* extract file name
* tags file name
* regions file name
* sequence number (e.g. 78123)

It will upload filtered objects to the database, and also create a table for tracking
the replication sequence.

To update data to the current sequence number, run `run_osc.sh`. It needs three
arguments: db name, tags and regions file names. When done, check out `osc_tracker`
table in the database.

### Usage with Augmented Diffs

Run `init.sh` with a database name: it will create a timestamp tracking table.
The optional second parameter should be an UTC (!) timestamp in the past.

Then run `run.sh` once. Fix all errors, wait until it catches up and run it again
(because catching up can take 1 day for 3 days passed). And then add it to crontab.
I recommend running it once or twice an hour, for the last hour of changes is cached
at the Overpass API server.

### Exporting statistics

Basically run `stats.sh` with a database name and an optional resulting html file name.

Alternatively, if you want a CSV, either add `--csv` key into the script, or manually
dump the `osc_tracker` / `adiff_tracker` to a CSV and then process it with the
`generate_user_stats.py` script.

## Author and License

Writter by Ilya Zverev, published under WTFPL (and MIT, choose what you like).
