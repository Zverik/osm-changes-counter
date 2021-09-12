# OpenStreetMap Changes Counter

These scripts were made for [this competition](http://osm-competition.tilda.ws/).
We count number of modified crossings and road segments per user per `place=city`.
And with that we rank them to determine who did the most work.

## Usage

You will need a PostgreSQL database. PostGIS is not needed. And Python 3.5+.

Run `init.sh` with a database name: it will create a timestamp tracking table.
The optional second parameter should be an UTC (!) timestamp in the past.

If you need cities other than russian 100k+ cities, import your area to PostgreSQL
with [osm2pgsql](https://www.osm2pgsql.org/) and
[style for places](https://github.com/Zverik/city-mapping-stats/blob/main/scripts/highways-and-places.style),
and run `prepare_cities.sh`.

Then run `run.sh` once. Fix all errors, wait until it catches up and run it again
(because catching up can take 1 day for 3 days passed). And then add it to crontab.
I recommend running it once or twice an hour, for the last hour of changes is cached
at the Overpass API server.

## Author and License

Writter by Ilya Zverev, published under WTFPL (and MIT, choose what you like).
