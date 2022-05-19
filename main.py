import psycopg2
import csv

conn = psycopg2.connect(database="practica1-sio-test", user="postgres", password="elprimo", host="127.0.0.1", port="5432")
cur = conn.cursor()

def crearTaulesPetites(catalegsCSV, index):
    #Comprovem la taula a realitzar
    if index == 3:
        print("Taula Directors")
        elimina_taula = "DROP TABLE directors;"
        crea_taula = "CREATE TABLE directors (ID_director SERIAL PRIMARY KEY, nom_director VARCHAR(400));"
        insert_registre = "INSERT INTO directors (nom_director) VALUES (%s)"
        validar_registre = "SELECT nom_director FROM directors WHERE nom_director=%s"
    elif index == 4:
        print("Taula Actors")
        elimina_taula = "DROP TABLE actors;"
        crea_taula = "CREATE TABLE actors (ID_actor SERIAL PRIMARY KEY, nom_actor VARCHAR(400));"
        insert_registre = "INSERT INTO actors (nom_actor) VALUES (%s)"
        validar_registre = "SELECT nom_actor FROM actors WHERE nom_actor=%s"
    elif index == 5:
        print("Taula paisos")
        elimina_taula = "DROP TABLE paisos;"
        crea_taula = "CREATE TABLE paisos (ID_pais SERIAL PRIMARY KEY, nom_pais VARCHAR(400));"
        insert_registre = "INSERT INTO paisos (nom_pais) VALUES (%s)"
        validar_registre = "SELECT nom_pais FROM paisos WHERE nom_pais=%s"
    elif index == 10:
        print("Taula Generes")
        elimina_taula = "DROP TABLE generes;"
        crea_taula = "CREATE TABLE generes (ID_genere SERIAL PRIMARY KEY, nom_genere VARCHAR(400));"
        insert_registre = "INSERT INTO generes (nom_genere) VALUES (%s)"
        validar_registre = "SELECT nom_genere FROM generes WHERE nom_genere=%s"
    else: exit

    #cur.execute(elimina_taula)
    cur.execute(crea_taula)

    if index == 3:
        cur.execute("INSERT INTO directors (ID_director, nom_director) VALUES (0, NULL)")
    if index == 4:
        cur.execute("INSERT INTO actors (ID_actor, nom_actor) VALUES (0, NULL)")
    if index == 5:
        cur.execute("INSERT INTO paisos (ID_pais, nom_pais) VALUES (0, NULL)")
    if index == 10:
        cur.execute("INSERT INTO generes (ID_genere, nom_genere) VALUES (0, NULL)")


    for catalegs in catalegsCSV:
        with open(catalegs, newline='\n') as File:
            File.readline() #evitem la primera linia
            reader = csv.reader(File)
            for row in reader:
                posicions = ",".join(row).split(";")    #separem camps per ; i despres per ,
                # comprovem quantitat parametrs
                if len(posicions) > 12:
                    ID = posicions[0].replace('s', '')
                    if catalegs == '1_cataleg_netflix.csv':
                        Platform = "Netflix"
                    if catalegs == '2_cataleg_amazon_prime.csv':
                        Platform = "AmazonPrime"
                    if catalegs == '3_cataleg_disney_plus.csv':
                        Platform = "DisneyPlus"
                    if catalegs == '4_cataleg_hulu.csv':
                        Platform = "Hulu"
                    print("ERROR:" + str(Platform+ID) + ", TOTAL POSICIONES:" +str(len(posicions)))
                else:
                        registre = posicions[index].split(",")
                        if str(registre) != "['']":     #comprovem que no esta buit
                            i = 0
                            while i < len(registre):    #inserim els diversos camps
                                insert_line = registre[i].lstrip(),
                                cur.execute(validar_registre, insert_line)
                                existeix = str(cur.fetchall())
                                if(existeix == '[]'):
                                    #print(insert_line)
                                    cur.execute(insert_registre, insert_line)
                                i = i+1

def crearTaulesGrans(catalegsCSV):

    eliminar_taula_tvshows = "DROP TABLE tvshows"
    crear_taula_tvshows = "CREATE TABLE tvshows(show_id SERIAL PRIMARY KEY, show_title VARCHAR(400), director INTEGER, FOREIGN KEY (director) REFERENCES directors (ID_director), casting INTEGER, FOREIGN KEY (casting) REFERENCES actors (ID_actor), countries INTEGER, FOREIGN KEY (countries) REFERENCES paisos (ID_pais), date_added DATE, release_year INTEGER, parental_guideline VARCHAR(10), duration VARCHAR(15), genres INTEGER, FOREIGN KEY (genres) REFERENCES generes (ID_genere), description VARCHAR(2000), cataleg VARCHAR(15));"
    eliminar_taula_movies = "DROP TABLE movies"
    crear_taula_movies = "CREATE TABLE movies(show_id SERIAL PRIMARY KEY, show_title VARCHAR(400), director INTEGER, FOREIGN KEY (director) REFERENCES directors (ID_director), casting INTEGER, FOREIGN KEY (casting) REFERENCES actors (ID_actor), countries INTEGER, FOREIGN KEY (countries) REFERENCES paisos (ID_pais), date_added DATE, release_year INTEGER, parental_guideline VARCHAR(10), duration VARCHAR(15), genres INTEGER, FOREIGN KEY (genres) REFERENCES generes (ID_genere), description VARCHAR(2000), cataleg VARCHAR(15));"

    #cur.execute(eliminar_taula_tvshows)
    cur.execute(crear_taula_tvshows)

    #cur.execute(eliminar_taula_movies)
    cur.execute(crear_taula_movies)

    select_director = "SELECT ID_director, nom_director FROM directors WHERE nom_director=%s"
    select_actor = "SELECT ID_actor, nom_actor FROM actors WHERE nom_actor=%s"
    select_pais = "SELECT ID_pais, nom_pais FROM paisos WHERE nom_pais=%s"
    select_genres = "SELECT ID_genere, nom_genere FROM generes WHERE nom_genere=%s"

    insertar_tvshows = "INSERT INTO tvshows (show_title, director, casting, countries, date_added, release_year, parental_guideline, duration, genres, description, cataleg) VALUES (%s, %s, %s, %s, TO_DATE(%s, 'DD/MM/YYYY'), %s, %s, %s, %s, %s, %s);"
    insertar_movies = "INSERT INTO movies (show_title, director, casting, countries, date_added, release_year, parental_guideline, duration, genres, description, cataleg) VALUES (%s, %s, %s, %s, TO_DATE(%s, 'DD/MM/YYYY'), %s, %s, %s, %s, %s, %s);"

    for catalegs in catalegsCSV:
        with open(catalegs, newline='\n') as File:
            File.readline()
            reader = csv.reader(File)
            for row in reader:
                posicions = ",".join(row).split(";")

                ID = posicions[0].replace('s', '')
                Type = posicions[1]
                Title = posicions[2]
                Director = posicions[3]
                Cast = posicions[4]
                Countries = posicions[5]
                DateAdded = posicions[6]
                RealeaseYear = posicions[7]
                ParentalGuideline = posicions[8]
                Duration = posicions[9]
                Genres = posicions[10]
                Description = posicions[11]
                if catalegs == '1_cataleg_netflix.csv':
                    Platform = "Netflix"
                if catalegs == '2_cataleg_amazon_prime.csv':
                    Platform = "AmazonPrime"
                if catalegs == '3_cataleg_disney_plus.csv':
                    Platform = "DisneyPlus"
                if catalegs == '4_cataleg_hulu.csv':
                    Platform = "Hulu"

                llistat_directors = Director.split(",")
                llistat_actors = Cast.split(",")
                llistat_paisos = Countries.split(",")
                llistat_genres = Genres.split(",")

                if RealeaseYear == "":
                    RealeaseYear = 0

                if len(posicions) > 12:
                    print("ERROR:" + str(Platform+ID) + ", TOTAL POSICIONES:" + str(len(posicions)))
                else:
                    for director in llistat_directors:
                        if director != "":
                            cur.execute(select_director, (director.lstrip(),))
                            director_output = cur.fetchall()
                            director_output_id = str(director_output).replace("(", "").replace("[", "").split(",")[0]

                        for actor in llistat_actors:
                            if actor != "":
                                cur.execute(select_actor, (actor.lstrip(), ))
                                actor_output = cur.fetchall()
                                actor_output_id = str(actor_output).replace("(", "").replace("[", "").split(",")[0]
                            else:
                                actor_output_id = 0
                            for pais in llistat_paisos:
                                if pais != "":
                                    cur.execute(select_pais, (pais.lstrip(),))
                                    pais_output = cur.fetchall()
                                    pais_output_id = str(pais_output).replace("(", "").replace("[", "").split(",")[0]

                                for genres in llistat_genres:
                                    if genres != "":
                                        cur.execute(select_genres, (genres.lstrip(),))
                                        genres_output = cur.fetchall()
                                        genres_output_id = str(genres_output).replace("(", "").replace("[", "").split(",")[0]

                                    print(director.lstrip(), actor.lstrip(), pais.lstrip(), genres.lstrip())
                                    insertar_linia = Title, director_output_id, actor_output_id, pais_output_id, DateAdded, RealeaseYear, ParentalGuideline, Duration, genres_output_id, Description, Platform
                                    if Type == "Movie":
                                        cur.execute(insertar_movies, insertar_linia)
                                    else:
                                        cur.execute(insertar_tvshows, insertar_linia)


def crearTaulaTomatoes(ratingsCSV):
    eliminar_taula_tomatoes = "DROP TABLE rottenTomatoes"
    crear_taula_tomatoes = "CREATE TABLE rottenTomatoes(tomatoes_id SERIAL PRIMARY KEY, show_title VARCHAR(400) NOT NULL, RealeaseYear INTEGER, tomatometer_status VARCHAR(200), tomatometer_rating INTEGER, audience_rating INTEGER, fresh_critics INTEGER, rotten_critics INTEGER, critics_count INTEGER, top_critics_count INTEGER);"
    insert_tomatoes = "INSERT INTO rottenTomatoes (show_title, RealeaseYear, tomatometer_status, tomatometer_rating, audience_rating, fresh_critics, rotten_critics, critics_count, top_critics_count) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s);"

    #cur.execute(eliminar_taula_tomatoes)
    cur.execute(crear_taula_tomatoes)

    with open(ratingsCSV, newline='\n') as File:
        File.readline()
        reader = csv.reader(File)
        for row in reader:

            posicions = ",".join(row).split(";")

            Title = posicions[0]
            RealeaseYear = posicions[1]
            if RealeaseYear == '':
                RealeaseYear = 0
            TomatometerStatus = posicions[2]
            if TomatometerStatus == '':
                TomatometerStatus = ""
            RatingTomatometer = posicions[3]
            if RatingTomatometer == '':
                RatingTomatometer = 0
            RatingAudience = posicions[4]
            if RatingAudience == '':
                RatingAudience = 0
            FreshCriticsCount = posicions[5]
            if FreshCriticsCount == '':
                FreshCriticsCount = 0
            RottenCriticsCount = posicions[6]
            if RottenCriticsCount == '':
                RottenCriticsCount = 0
            CriticsCount = posicions[7]
            if CriticsCount == '':
                CriticsCount = 0
            TopCriticsCount = posicions[8]
            if TopCriticsCount == '':
                TopCriticsCount = 0

            if len(posicions) > 9:
                 print("ERROR:" + str(posicions))
            else:
                insertar_linia = Title, RealeaseYear, TomatometerStatus, RatingTomatometer, RatingAudience, FreshCriticsCount, RottenCriticsCount, CriticsCount, TopCriticsCount
                print(insertar_linia)
                cur.execute(insert_tomatoes, insertar_linia)

def crearTaulaFilmTV(ratingsCSV):
    eliminar_taula_filmTV = "DROP TABLE filmTV"
    crear_taula_filmTV = "CREATE TABLE filmTV (filmTV_id SERIAL PRIMARY KEY, show_title VARCHAR(400) NOT NULL, RealeaseYear INTEGER, rating_avg DECIMAL, rating_critics DECIMAL, rating_audience INTEGER, votes_count INTEGER);"
    insertar_filmTV = "INSERT INTO filmTV (show_title, RealeaseYear, rating_avg, rating_critics, rating_audience, votes_count) VALUES (%s, %s, %s, %s, %s, %s);"

    #cur.execute(eliminar_taula_filmTV)
    cur.execute(crear_taula_filmTV)

    with open(ratingsCSV, newline='\n') as File:
            File.readline()
            reader = csv.reader(File)
            for row in reader:
                posicions = ",".join(row).split(";")

                Title = posicions[0]
                RealeaseYear = posicions[1]
                if RealeaseYear == '':
                    RealeaseYear = 0
                RatingAVG = posicions[2].replace(",", ".")
                if RatingAVG == '':
                    RatingAVG = 0
                RatingCritics = posicions[3].replace(",", ".")
                if RatingCritics == '':
                    RatingCritics = 0
                RatingAudience = posicions[4]
                if RatingAudience == '':
                    RatingAudience = 0
                VotesCount = posicions[5]
                if VotesCount == '':
                    VotesCount = 0

                if len(posicions) > 6:
                    print("ERROR:" + str(posicions))
                else:
                    insertar_linia = Title, RealeaseYear, RatingAVG, RatingCritics, RatingAudience, VotesCount
                    print(insertar_linia)
                    cur.execute(insertar_filmTV, insertar_linia)



if __name__ == '__main__':


    catalegsCSV = ['1_cataleg_netflix.csv', '2_cataleg_amazon_prime.csv', '3_cataleg_disney_plus.csv', '4_cataleg_hulu.csv']
    tomatoesCSV = '5_ratings_rotten_tomatoes.csv'
    filmTvCSV = '6_ratings_filmtv.csv'

    for x in {3, 4, 5, 10}:
        crearTaulesPetites(catalegsCSV, x)

    crearTaulesGrans(catalegsCSV)

    crearTaulaTomatoes(tomatoesCSV)
    crearTaulaFilmTV(filmTvCSV)

    conn.commit()
    cur.close()
    conn.close()





