use std::{
    env,
    fs::File,
    io::BufReader,
    time::{Duration, SystemTime}, path::Path,
};

use clap::{Parser, Subcommand};
use csv::StringRecord;
use diesel::{
    r2d2::{Builder, ConnectionManager, Pool, PooledConnection},
    PgConnection, QueryDsl, RunQueryDsl,
};
use diesel::{table, Insertable};
use diesel_migrations::{embed_migrations, EmbeddedMigrations, MigrationHarness};
use dotenv::dotenv;
use postgis_diesel::types::Point;
use rand::{thread_rng, Rng};
use serde::Deserialize;

pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!();

fn run_migration(conn: &mut PooledConnection<ConnectionManager<PgConnection>>) {
    conn.run_pending_migrations(MIGRATIONS)
        .expect("migration failure");
}

fn init_connection_pool() -> Pool<ConnectionManager<PgConnection>> {
    dotenv().ok();
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL not set");
    let cm = ConnectionManager::new(database_url);
    Builder::new()
        .max_size(20)
        .min_idle(Some(1))
        .max_lifetime(Some(Duration::from_secs(30)))
        .build(cm)
        .expect("can't create connection pool!")
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Upload data to postgres DB
    Upload,
    /// Execute sequentially 500 requests to find 500 nearest neighbors in a loop
    Bench,
}

fn main() {
    let cli = Cli::parse();

    let c_pool = init_connection_pool();
    match cli.command {
        Commands::Upload => insert_data(&c_pool),
        Commands::Bench => bench_get(&c_pool),
    }
}

fn bench_get(c_pool: &Pool<ConnectionManager<PgConnection>>) {
    let mut rng = thread_rng();
    let mut conn = c_pool.get().unwrap();
    let start = SystemTime::now();
    for _ in 0..500 {
        cities::table
            .order_by(postgis_diesel::operators::distance_2d(
                cities::location,
                Point::new(
                    rng.gen_range(-90.0..90.0),
                    rng.gen_range(-180.0..180.0),
                    Some(4326),
                ),
            ))
            .limit(500)
            .execute(&mut conn)
            .expect("nothing found");
    }
    println!("elapsed {:?}", start.elapsed())
}

fn insert_data(c_pool: &Pool<ConnectionManager<PgConnection>>) {
    let mut conn = c_pool.get().expect("can't get connection");
    run_migration(&mut conn);
    let batch_size = 10000;
    let f = File::open("./data/cities.txt.zip")
        .expect("can't open cities file, expected location ./data/cities.txt.zip");
    let reader = BufReader::new(f);
    let mut zip_reader = zip::ZipArchive::new(reader).unwrap();
    let data_path = Path::new("./");
    zip_reader.extract(data_path).unwrap();
    let mut rdr = csv::Reader::from_reader(BufReader::new(File::open("./data/cities.txt").unwrap()));
    let mut cities = Vec::with_capacity(batch_size);
    let mut batch_counter = 0;

    for result in rdr.deserialize::<CityRecord>() {
        cities.push(NewCity::from(result.unwrap()));
        if cities.len() == batch_size {
            println!("inserting {} batch", batch_counter);
            diesel::insert_into(cities::table)
                .values(&cities)
                .execute(&mut conn)
                .expect("can't insert cities");
            cities.clear();
            batch_counter += 1;
        }
    }
    if !cities.is_empty() {
        batch_counter += 1;
        println!("inserting {} batch", batch_counter);
        diesel::insert_into(cities::table)
            .values(&cities)
            .execute(&mut conn)
            .expect("can't insert cities");
    }
}

#[derive(Insertable)]
#[diesel(table_name=cities)]
struct NewCity {
    country: String,
    city: String,
    accent_city: String,
    region: String,
    location: Point,
}

impl From<CityRecord> for NewCity {
    fn from(cr: CityRecord) -> Self {
        NewCity {
            country: cr.country,
            city: cr.city,
            accent_city: cr.accent_city,
            region: cr.region,
            location: Point::new(cr.longitude, cr.latitude, Some(4326)),
        }
    }
}

impl From<&StringRecord> for NewCity {
    fn from(cr: &StringRecord) -> Self {
        NewCity {
            country: cr.get(0).unwrap().to_string(),
            city: cr.get(1).unwrap().to_string(),
            accent_city: cr.get(2).unwrap().to_string(),
            region: cr.get(3).unwrap().to_string(),
            location: Point::new(
                cr.get(5).unwrap().parse().unwrap(),
                cr.get(4).unwrap().parse().unwrap(),
                Some(4326),
            ),
        }
    }
}

#[derive(Debug, Deserialize)]
struct CityRecord {
    #[serde(alias = "Country")]
    country: String,
    #[serde(alias = "City")]
    city: String,
    #[serde(alias = "Accent City")]
    accent_city: String,
    #[serde(alias = "Region")]
    region: String,
    #[serde(alias = "Latitude")]
    latitude: f64,
    #[serde(alias = "Longitude")]
    longitude: f64,
}

table! {
    use postgis_diesel::sql_types::*;
    use diesel::sql_types::*;
    cities (id) {
        id -> Int4,
        country -> Text,
        city -> Text,
        accent_city -> Text,
        region -> Text,
        location -> Geometry,
    }
}
