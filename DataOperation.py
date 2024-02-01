import psycopg2
import pandas as pd


class DataOperation:
    def __init__(self, host, db, uname, pwd):
        self.connection = psycopg2.connect(
            host=host,
            database=db,
            user=uname,
            password=pwd
        )
        self.cursor = self.connection.cursor()

    def write_data_to_db(self, entity, idx, attribute, leng, linear, quadratic, sine):

        query = ("INSERT INTO tier2.curvfit select '" + entity + "','" + idx + "','" + attribute + "','" + leng + "','"
                 + str(linear[0]) + "','" + str(linear[1])+ "','"
                 + str(quadratic[0]) + "','" + str(quadratic[1]) + "','" + str(quadratic[2]) + "','"
                 + str(sine[0]) + "','" + str(sine[1]) + "','" + str(sine[2]) + "','" + str(sine[3]) + "';")

        print(query)
        self.cursor.execute(query)

        self.connection.commit()

    def fetch_data_from_db(self, query):
        # Example query
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        columns = [desc[0] for desc in self.cursor.description]

        # Create a Pandas DataFrame
        ans = pd.DataFrame(result, columns=columns)
        return ans

    def close_db_connection(self):
        # Close the cursor and connection
        self.cursor.close()
        self.connection.close()
