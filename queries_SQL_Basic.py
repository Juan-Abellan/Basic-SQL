import sqlite3

connection = sqlite3.connect('data/movies.sqlite')
connection.row_factory = sqlite3.Row
cursor = connection.cursor()


def database_explorer(db_cursor):
    """ Shows the number of tables in the database"""

    query = """
            SELECT name FROM sqlite_master  
            WHERE type='table';
            """
    db_cursor.execute(query)
    rows = db_cursor.fetchall()

    print(f"""
    {type(rows) = }
    {len(rows) = }
    """)

    return [row[row.keys()[0]] for row in rows]


def directors_count(db_cursor):
    """return the number of directors contained in the database"""

    query = """
            SELECT COUNT(*)  
            FROM directors;
            """

    db_cursor.execute(query)
    rows = db_cursor.fetchall()

    print(f"""
    {type(rows) = }
    {len(rows) = }
    {[row.keys() for row in rows]}
    """)

    return [row[row.keys()[0]] for row in rows][0]


def directors_list(db_cursor):
    """return the list of all the directors sorted in alphabetical order"""

    query = """
                SELECT directors.name  
                FROM directors
                ORDER BY directors.name ASC;
                """

    db_cursor.execute(query)
    rows = db_cursor.fetchall()

    print(f"""
    {type(rows) = }
    {len(rows) = }
    {[row.keys() for row in rows][0] = }
    """)

    return [row[row.keys()[0]] for row in rows]


def love_movies(db_cursor):
    """return the list of all movies which contain the exact word "love"
    in their title, sorted in alphabetical order"""

    query = """
            SELECT title
            FROM movies
            WHERE UPPER(title) LIKE '% LOVE %'
            OR UPPER(title) LIKE 'LOVE %'
            OR UPPER(title) LIKE '% LOVE'
            OR UPPER(title) LIKE 'LOVE'
            OR UPPER(title) LIKE '% LOVE''%'
            OR UPPER(title) LIKE '% LOVE.'
            OR UPPER(title) LIKE 'LOVE,%'
            ORDER BY title
            """

    db_cursor.execute(query)
    rows = db_cursor.fetchall()

    print(f"""
    {type(rows) = }
    {len(rows) = }
    {[row.keys() for row in rows][0] = }
    """)

    return [row[row.keys()[0]] for row in rows]


def directors_named_like_count(db_cursor, name):
    """return the number of directors which contain a given word in their name"""

    query = """
                SELECT COUNT(name) AS name_count
                FROM directors
                WHERE name LIKE ?
                """

    db_cursor.execute(query, (f"%{name}%",))
    rows = db_cursor.fetchall()

    print(f"""
    {type(rows) = }
    {len(rows) = }
    {[row.keys() for row in rows][0] = }
    """)

    return f"{name}: {[row[row.keys()[0]] for row in rows][0]}"


def movies_longer_than(db_cursor, minutes):
    """return this list of all movies which are longer than a given duration,
    sorted in the alphabetical order"""
    query = """
            SELECT title,  minutes
            FROM movies                       
            WHERE minutes >= ? 
            ORDER BY title     
            """

    # ['title', 'rating', 'vote_count', 'start_year', 'minutes', 'genres', 'imdb_id', 'id', 'director_id']

    db_cursor.execute(query, (f"{minutes}",))
    rows = db_cursor.fetchall()

    print(f"""
    {type(rows) = }
    {len(rows) = }
    {[row.keys() for row in rows][0] = }
    """)

    return [{"title": row["title"],
             "minutes": row["minutes"]} for row in rows]


def detailed_movies(db_cursor):
    """return the list of movies with their genres and director name"""
    query = db_cursor.execute(
        """
        SELECT directors.name, movies.title, movies.genres FROM movies
        JOIN directors ON directors.id = movies.director_id
        """
    )
    rows = query.fetchall()

    print(f"""
    {type(rows) = }
    {len(rows) = }
    {[row.keys() for row in rows][0] = }
    """)

    return [[row[key] for key in row.keys()] for row in rows]


# ----------------------------------------------------------------------------------------------------
def late_released_movies(db_cursor):
    """return the list of dictionaries with all movies released after their director death"""
    query = db_cursor.execute(
        """
        SELECT title, directors.name
        FROM directors
        JOIN movies ON movies.director_id = directors.id
        WHERE start_year > death_year
        /*LIMIT 5*/
        ORDER BY title
        """
    )
    rows = query.fetchall()

    print(f"""
       {type(rows) = }
       {len(rows) = }
       {[row.keys() for row in rows][0] = }
       """)
    key_list = [row.keys() for row in rows][0]
    return [{key_list[0]: row[key_list[0]], key_list[1]: row[key_list[1]]} for row in rows]


def stats_on(db_cursor, genre_name):
    """return a dict of stats for a given genre"""
    query = db_cursor.execute(
        """
        SELECT genres, COUNT(*) AS number_of_movies,
        ROUND(AVG(minutes), 2) AS avg_length
        FROM movies
        WHERE genres = ?
        """,
        (genre_name,)
    )
    rows = query.fetchall()

    print(f"""
           {type(rows) = }
           {len(rows) = }
           {[row.keys() for row in rows][0] = }
           """)

    keys = [row.keys() for row in rows][0]
    stats_dict = {}

    for key in keys:
        for row in rows:
            stats_dict[key] = row[key]

    return stats_dict


def top_five_directors_for(db_cursor, genre_name):
    """return the top 5 of the directors with the most movies for a given genre"""
    query = db_cursor.execute(
        """
        SELECT name, COUNT(title) AS movie_count
        FROM movies
        JOIN directors ON directors.id = movies.director_id
        WHERE genres = ?
        GROUP BY name
        ORDER BY movie_count DESC, name        
        LIMIT 5
        """,
        (genre_name,)
    )
    rows = query.fetchall()

    print(f"""
           {type(rows) = }
           {len(rows) = }
           {[row.keys() for row in rows][0] = }
           """)
    return [[row[key] for key in row.keys()] for row in rows]


print(f"""
-------------------------------------------------------------------------------------------
database_explorer(db_cursor=cursor) = 
directors_count(db_cursor=cursor) = 
directors_list(db_cursor=cursor) = 
love_movies(db_cursor=cursor) = 
directors_named_like_count(db_cursor=cursor, name="Carlos") = 
movies_longer_than(db_cursor=cursor, minutes=700) = 
detailed_movies(db_cursor=cursor) = 
detailed_movies(db_cursor=cursor) = 
late_released_movies(db_cursor=cursor) = 
stats_on(db_cursor=cursor, genre_name="Comedy") =
{top_five_directors_for(db_cursor=cursor, genre_name="Comedy") = }
""")
