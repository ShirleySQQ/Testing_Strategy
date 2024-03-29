import csv
import os
import psycopg2
from psycopg2 import ProgrammingError

# read from config file will be great
# summary each teams' total matches, win matches and loss matches then write to a csv file

csv_file = 'C:\SQQ\Testing_Strategy\\ICC_tournament.csv'
table_name = 'public."ICC_tournament"'
table_schema = '( "Team_1" text COLLATE pg_catalog."default", "Team_2" text COLLATE pg_catalog."default" ,' \
               '"Winner" text COLLATE pg_catalog."default")' \
               'WITH ( OIDS = FALSE) TABLESPACE pg_default;'
safe_name = table_name.replace('.', '_').replace('"', '_')
summarize_file = f"C:\SQQ\Testing_Strategy\\{safe_name}_summary.csv"
print(format(table_name))

class write_to_db():
    print(os.environ)
    try:
        conn = psycopg2.connect(
            database='postgres',
            user='postgres',
            password="123456",
            host='localhost',
            port='5432'
        )
        # print(os.path())
        # print(os.getenv('AIRFLOW_HOME'))

        cur = conn.cursor()
        # step1 create table is not exists
        # create_sql = ' CREATE TABLE IF NOT EXISTS {table_name} {table_schema}'
        cur.execute(f"CREATE TABLE IF NOT EXISTS {table_name} {table_schema}")
        print('table records: ', cur.execute(f"SELECT count(*) FROM {table_name}"))
        # step2 delete records of table
        cur.execute(f"delete from {table_name}")
        # step 3 load records from csv to table
        with open(csv_file, 'r') as f:
            next(f)
            csv_reader = csv.reader(f)
            for row in csv_reader:
                # table columns how to control this?
                cur.execute(f"insert into {table_name} values (%s,%s,%s);", row)
            try:
                conn.commit()
            except ProgrammingError:
                print('Error occurred, rolling back transaction.')
                conn.rollback()
            else:
                print('Transaction committed.')
            cur.execute(f"SELECT count(*) FROM {table_name}")
            rows = cur.fetchall()
            print(rows)
    except psycopg2.Error as e:
        print('Error: ', e)
        print('write to db failed.')
    else:
        print('Data imported successfully.')
        # step 4 run a logic
        cur.execute(f'''
        with team_name as (
	select  "Team_1" as Team_Name
	from {table_name}
	union 
	select distinct "Team_2" as Team_Name
	from {table_name}
		),
		team_matches as (
		select a.Team_Name,
			case when b."Team_1" is not null then 1
				  else 0 end as team1_no,
			case when c."Team_2" is not null then 1
				  else 0  end as team2_no,
			case when b."Winner" =a.team_name then 1
				  else 0 end as winner1_no,
			case when c."Winner" =a.team_name then 1
				  else 0 end as winner2_no
			from team_name a 
			left join
			{table_name} b
			on a.team_name = b."Team_1"
			left join
			{table_name} c
			on a.team_name = c."Team_2"
		
		)		
		select  Team_name,sum(team1_no+team2_no) as matches_played,
		sum(winner1_no+winner2_no) as no_of_wins,
		(sum(team1_no+team2_no)-sum(winner1_no+winner2_no) )as no_of_losses		
		from team_matches group by team_name order by team_name
        
       ''')
        rows = cur.fetchall()
        with open(summarize_file, mode='x', newline='') as csvfile:
            columns_name = [desc[0] for desc in cur.description]
            writer = csv.writer(csvfile)
            writer.writerow(columns_name)
            for row in rows:
                writer.writerow(row)

    finally:
        cur.close()
        conn.close()


'''
engine = create_engine('postgresql://postgres:123456@localhost:5432/postgres')
try:
    data = pd.read_csv(csv_file)

    print(data.count())
    print(engine.connect().execute(text(f"SELECT * FROM  {table_name}")).fetchone())
    with engine.begin() as connection:
        data.to_sql(name=table_name, con=connection, if_exists='append', index=False)
except Exception as e:
    print({e})
finally:
    engine.dispose()

engine.dispose()
'''
