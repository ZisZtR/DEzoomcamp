import pandas as pd
from sqlalchemy import create_engine
import click

url = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv'

@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--table-name', default='zones', help='Target table name')

def run(pg_user, pg_pass, pg_host, pg_port, pg_db, table_name):

    df_zones = pd.read_csv(url)
    # df_zones.head(5)

    engine = create_engine(f'postgresql+psycopg://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}') #Connect DB


    df_zones.head(n=0).to_sql(name=f'{table_name}', con=engine, if_exists='replace') # insert only header in db

    df_zones.to_sql(name=f'{table_name}', con=engine, if_exists='append')


if __name__ == '__main__':
    run()