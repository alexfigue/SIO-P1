import psycopg2
import matplotlib.pyplot as plt
from datetime import datetime

def obrir_connexio():
    conn = psycopg2.connect(database="practica1-sio-test", user="postgres", password="postgres", host="127.0.0.1", port="5432")
    cur = conn.cursor()
    return conn, cur

def tancar_connexio(conn, cur):
    conn.commit()
    cur.close()
    conn.close()

def contingut_plataformes(plataformes):
    conn = obrir_connexio()[0]
    cur = obrir_connexio()[1]

    consulta_plataformes_tvshows = "SELECT COUNT(DISTINCT show_title) FROM tvshows WHERE cataleg=%s"
    consulta_plataformes_movies = "SELECT COUNT(DISTINCT show_title) FROM movies WHERE cataleg=%s"
    count_plataformes = []
    count_tvshows = []
    count_movies = []

    for plataforma in plataformes:
        cur.execute(consulta_plataformes_tvshows, (plataforma, ))
        tvshows = cur.fetchall()[0][0]
        count_tvshows.append(tvshows)
        cur.execute(consulta_plataformes_movies, (plataforma, ))
        movies = cur.fetchall()[0][0]
        count_movies.append(movies)
        #print("TVShows: " + str(tvshows) + " | Movies: " + str(movies))
        tupla = tvshows + movies, plataforma
        count_plataformes.append(tupla)

    print("PLATAFORMES-------------------")
    for linia in count_plataformes:
        print(linia)

    print("------------------------------")
    print("Plataforma amb més contingut: " + str(max(count_plataformes)[1]) + " --> " + str(max(count_plataformes)[0]))
    print("Plataforma amb menys contingut: " + str(min(count_plataformes)[1]) + " --> " + str(min(count_plataformes)[0]))

    plataformes_fig = [fila[1] for fila in count_plataformes]
    total_fig = [fila[0] for fila in count_plataformes]
    fig, ax = plt.subplots()
    ax.bar(plataformes_fig, total_fig)
    plt.title("Contingut de les diferents plataformes:")
    #plt.xlabel("Plataformes")
    #plt.ylabel("Quantitat")
    plt.show()

    tancar_connexio(conn, cur)

def contingut_separat_plataformes(plataformes):
    conn = obrir_connexio()[0]
    cur = obrir_connexio()[1]

    consulta_plataformes_tvshows = "SELECT COUNT(DISTINCT show_title) FROM tvshows WHERE cataleg=%s"
    consulta_plataformes_movies = "SELECT COUNT(DISTINCT show_title) FROM movies WHERE cataleg=%s"

    count_plataformes = []
    count_tvshows = []
    count_movies = []

    for plataforma in plataformes:
        cur.execute(consulta_plataformes_tvshows, (plataforma,))
        tvshows = cur.fetchall()[0][0]
        cur.execute(consulta_plataformes_movies, (plataforma,))
        movies = cur.fetchall()[0][0]
        print("Plataforma: " + plataforma + " --> " + "TVShows: " + str(tvshows) + " | Movies: " + str(movies))
        count_plataformes.append(plataforma)
        count_movies.append(movies)
        count_tvshows.append(tvshows)

    plt.bar(count_plataformes, count_tvshows, label='TV Shows')
    plt.bar(count_plataformes, count_movies, label='Movies', bottom=count_tvshows)
    plt.title("Distribució contingut de les diferents plataformes:")
    plt.legend()
    plt.show()

    tancar_connexio(conn, cur)
    
def directors_mes_repetits():
    conn = obrir_connexio()[0]
    cur = obrir_connexio()[1]

    num_directors = []

    MAX_VALUE = 0
    tupla = None

    consulta_id_directors = "SELECT * FROM directors"
    consulta_directors_tvshows = "SELECT DISTINCT show_title, director FROM tvshows ORDER BY director DESC"
    consulta_directors_movies = "SELECT DISTINCT show_title, director FROM movies ORDER BY director DESC"
    consulta_nom_director = "SELECT nom_director FROM directors WHERE id_director=%s"

    cur.execute(consulta_directors_tvshows, ())
    directors_tvshows = cur.fetchall()

    cur.execute(consulta_directors_movies, ())
    directors_movies = cur.fetchall()

    cur.execute(consulta_id_directors, ())
    id_directors = cur.fetchall()

    for director in directors_tvshows:
        num_directors.append(director[1])


    for director in directors_movies:
        num_directors.append(director[1])

    top_directors = []
    for director in id_directors:
        if director[0] != 0:
            tupla = num_directors.count(director[0]), director[0]
            top_directors.append(tupla)

    top = sorted(top_directors, key=lambda x : x[0], reverse=True)
    top = top[0:10]
    #print(top)

    noms = []

    for director in top:
        #print(director[1])
        cur.execute(consulta_nom_director, (director[1], ))
        nom_director = cur.fetchall()[0][0]
        noms.append(nom_director)

    quantitat_fig = [fila[0] for fila in top]
    fig, ax = plt.subplots(figsize=(15, 6))
    ax.barh(noms, quantitat_fig)
    plt.title("Directors més repetits:")
    plt.show()

    #print(tupla) 
    
    tancar_connexio(conn, cur)
    
def actors_mes_repetits():
    conn = obrir_connexio()[0]
    cur = obrir_connexio()[1]

    num_actors = []

    MAX_VALUE = 0
    tupla = None

    #consulta_directors = "SELECT director, COUNT(director) AS total_tvshows FROM tvshows GROUP BY director ORDER BY total_tvshows DESC"
    consulta_id_actors = "SELECT * FROM actors WHERE id_actor!=0"
    consulta_actors_tvshows = "SELECT DISTINCT show_title, casting FROM tvshows ORDER BY casting DESC"
    consulta_actors_movies = "SELECT DISTINCT show_title, casting FROM movies ORDER BY casting DESC"
    consulta_nom_actors = "SELECT nom_actor FROM actors WHERE id_actor=%s"

    cur.execute(consulta_actors_tvshows, ())
    actors_tvshows = cur.fetchall()

    cur.execute(consulta_actors_movies, ())
    actors_movies = cur.fetchall()

    cur.execute(consulta_id_actors, ())
    id_actors = cur.fetchall()

    for actor in actors_tvshows:
        num_actors.append(actor[1])

    for actor in actors_movies:
        num_actors.append(actor[1])

    top_actors = []
    for actor in id_actors:
        tupla = num_actors.count(actor[0]), actor[0]
        top_actors.append(tupla)
        print(top_actors)

    top = sorted(top_actors, key=lambda x : x[0], reverse=True)
    top = top[0:10]
    print(top)

    noms = []

    for actor in top:
        cur.execute(consulta_nom_actors, (actor[1], ))
        nom_actors = cur.fetchall()[0][0]
        print(nom_actors)
        noms.append(nom_actors)

    quantitat_fig = [fila[0] for fila in top]
    fig, ax = plt.subplots(figsize=(15, 6))
    ax.barh(noms, quantitat_fig)
    plt.title("Actors més repetits:")
    plt.show()

    #print(tupla)

    tancar_connexio(conn, cur)

def duracio_pelis(plataformes):
    conn = obrir_connexio()[0]
    cur = obrir_connexio()[1]

    consulta_plataformes_movies = "SELECT DISTINCT show_title, duration FROM movies WHERE cataleg=%s"

    cont_60_mins = 0
    cont_120_mins = 0
    cont_mes_120_mins = 0

    dades = []

    for plataforma in plataformes:
        cur.execute(consulta_plataformes_movies, (plataforma, ))
        movies = cur.fetchall()

        total_min = 0

        for movie in movies:
            minuts = movie[1]

            if len(minuts) != 0:
                eq = minuts.split()
                if eq[1] == "min":
                    total_min = total_min + int(eq[0])
                    if int(eq[0]) <= 60:
                        cont_60_mins = cont_60_mins + 1
                    elif (int(eq[0]) > 60) and (int(eq[0]) <= 120):
                        cont_120_mins = cont_120_mins + 1
                    elif int(eq[0]) > 120:
                        cont_mes_120_mins = cont_mes_120_mins + 1

        total_pelis = cont_60_mins + cont_120_mins + cont_mes_120_mins
        print(total_pelis)

        mitjana = round(float(total_min/total_pelis), 2)
        print("Duració de les pel·licules de " + plataforma + " --> " + str(mitjana))

        dades.append(plataforma)
        dades.append(cont_60_mins)
        dades.append(cont_120_mins)
        dades.append(cont_mes_120_mins)
        dades.append(mitjana)

        cont_60_mins = 0
        cont_120_mins = 0
        cont_mes_120_mins = 0

    quesitos = ['Fins a 60 mins', 'Entre 60 i 120 mins', 'Més de 120 mins']
    print()
    for dada in dades:
        print(dada)

    cont = 0
    while cont < len(dades):
        plt.pie([int(dada) for dada in dades[(cont + 1): (cont + 4)]], labels=quesitos, explode=(0, 0, 0), autopct='%1.1f%%', shadow=False, startangle=90)
        plt.title("Distribució de duració de les pel·lícules de " + str(dades[cont]))
        plt.text(-1.5, -1.4, "Mitjana: " + str(dades[cont + 4]) + " minuts", fontsize=12)
        plt.show()
        cont = cont + 5

    tancar_connexio(conn, cur)

def duracio_series(plataformes):
    conn = obrir_connexio()[0]
    cur = obrir_connexio()[1]

    consulta_plataformes_tvshows = "SELECT DISTINCT show_title, duration FROM tvshows WHERE cataleg=%s"

    cont_3_seasons = 0
    cont_6_seasons = 0
    cont_mes_6_seasons = 0

    dades = []

    for plataforma in plataformes:
        cur.execute(consulta_plataformes_tvshows, (plataforma, ))
        movies = cur.fetchall()

        total_seas = 0

        for i in movies:
            seasons = i[1]

            if len(seasons) != 0:
                eq = seasons.split()
                if (eq[1] == "Season") or (eq[1] == "Seasons"):
                    total_seas = total_seas + int(eq[0])
                    if int(eq[0]) <= 3:
                        cont_3_seasons = cont_3_seasons + 1
                    elif (int(eq[0]) > 3) and (int(eq[0]) <= 6):
                        cont_6_seasons = cont_6_seasons + 1
                    elif int(eq[0]) > 6:
                        cont_mes_6_seasons = cont_mes_6_seasons + 1

        total_tvsh = cont_3_seasons + cont_6_seasons + cont_mes_6_seasons
        print(total_tvsh)

        mitjana = round(float(total_seas/total_tvsh), 2)
        print("Duració de les series de " + plataforma + " --> " + str(mitjana))

        dades.append(plataforma)
        dades.append(cont_3_seasons)
        dades.append(cont_6_seasons)
        dades.append(cont_mes_6_seasons)
        dades.append(mitjana)

        cont_3_seasons = 0
        cont_6_seasons = 0
        cont_mes_6_seasons = 0

    quesitos = ['Fins a 3 temporades', 'Entre 4 i 6 temporades', 'Més de 6 temporades']
    print()
    for dada in dades:
        print(dada)

    cont = 0
    while cont < len(dades):
        plt.pie([int(dada) for dada in dades[(cont + 1): (cont + 4)]], labels=quesitos, explode=(0, 0, 0), autopct='%1.1f%%', shadow=False, startangle=90)
        plt.title("Distribució de duració de les sèries de " + str(dades[cont]))
        plt.text(-1.5, -1.4, "Mitjana: " + str(dades[cont + 4]) + " temporades", fontsize=12)
        plt.show()
        cont = cont + 5

    tancar_connexio(conn, cur)

def generes_populars(plataformes):
    conn = obrir_connexio()[0]
    cur = obrir_connexio()[1]

    num_generes = []

    consulta_plataformes_generes_pelis = "SELECT DISTINCT show_title, genres FROM movies WHERE cataleg=%s"
    consulta_plataformes_generes_tvshows = "SELECT DISTINCT show_title, genres FROM tvshows WHERE cataleg=%s"
    consulta_generes = "SELECT * FROM generes"
    consulta_plataformes_nom_generes_pelis = "SELECT nom_genere FROM generes WHERE id_genere=%s"

    cur.execute(consulta_generes, ())
    id_genere = cur.fetchall()

    for plataforma in plataformes:
        cur.execute(consulta_plataformes_generes_pelis, (plataforma, ))
        generes_pelis = cur.fetchall()

        cur.execute(consulta_plataformes_generes_tvshows, (plataforma,))
        generes_tvshows = cur.fetchall()

        for genere in generes_pelis:
            num_generes.append(genere[1])

        for genere in generes_tvshows:
            num_generes.append(genere[1])

        top_generes = []
        for genere in id_genere:
            if genere != 0:
                tupla = num_generes.count(genere[0]), genere[0]
                #print(str(num_generes.count(genere[0])) + " id_genere: " + str(genere[0]))
                top_generes.append(tupla)

        top = sorted(top_generes, key=lambda x : x[0], reverse=True)
        top = top[0:5]
        print(plataforma)
        print(top)

        noms = []
        for genere in top:
            cur.execute(consulta_plataformes_nom_generes_pelis, (genere[1], ))
            nom_genere = cur.fetchall()[0][0]
            print(nom_genere)
            noms.append(nom_genere)

        print()

        quantitat_fig = [fila[0] for fila in top]
        fig, ax = plt.subplots(figsize=(14, 6))
        ax.barh(noms, quantitat_fig)
        plt.title("Top 5 gèneres més populars a la plataforma " + plataforma + ":")
        plt.show()

        num_generes.clear()

    tancar_connexio(conn, cur)

def països_mes_contingut():
    conn = obrir_connexio()[0]
    cur = obrir_connexio()[1]

    num_pais = []

    consulta_plataformes_paisos_series = "SELECT DISTINCT show_title, countries FROM tvshows"
    consulta_plataformes_paisos_pelis = "SELECT DISTINCT show_title, countries FROM movies"
    consulta_paisos = "SELECT * FROM paisos"
    consulta_plataformes_nom_paisos = "SELECT nom_pais FROM paisos WHERE id_pais=%s"


    cur.execute(consulta_plataformes_paisos_series, ())
    pais_series = cur.fetchall()

    cur.execute(consulta_plataformes_paisos_pelis, ())
    pais_pelis = cur.fetchall()

    cur.execute(consulta_paisos, ())
    id_pais = cur.fetchall()

    for pais in pais_series:
        num_pais.append(pais[1])

    for pais in pais_pelis:
        num_pais.append(pais[1])

    top_pais = []
    for pais in id_pais:
        if pais[0] != 0:
            tupla = num_pais.count(pais[0]), pais[0]
            #print(str(num_pais.count(pais[0])) + " country: " + str(pais[0]))
            top_pais.append(tupla)

    top = sorted(top_pais, key=lambda x: x[0], reverse=True)
    top = top[0:5]

    print(top)

    noms = []
    for pais in top:
        cur.execute(consulta_plataformes_nom_paisos, (pais[1], ))
        nom_pais = cur.fetchall()[0][0]
        noms.append(nom_pais)
        print(nom_pais)

    quantitat_fig = [fila[0] for fila in top]
    fig, ax = plt.subplots(figsize=(14, 6))
    ax.barh(noms, quantitat_fig)
    plt.title("Països que han produït més contingut:")
    plt.show()

    tancar_connexio(conn, cur)

def països_mes_contingut_plataformes(plataformes):
    conn = obrir_connexio()[0]
    cur = obrir_connexio()[1]

    num_pais = []

    consulta_plataformes_paisos_series = "SELECT DISTINCT show_title, countries FROM tvshows WHERE cataleg=%s"
    consulta_plataformes_paisos_pelis = "SELECT DISTINCT show_title, countries FROM movies WHERE cataleg=%s"
    consulta_paisos = "SELECT * FROM paisos"
    consulta_plataformes_nom_paisos = "SELECT nom_pais FROM paisos WHERE id_pais=%s"


    for plataforma in plataformes:
        cur.execute(consulta_plataformes_paisos_series, (plataforma, ))
        pais_series = cur.fetchall()

        cur.execute(consulta_plataformes_paisos_pelis, (plataforma,))
        pais_pelis = cur.fetchall()

        cur.execute(consulta_paisos, ())
        id_pais = cur.fetchall()

        for pais in pais_series:
            num_pais.append(pais[1])

        for pais in pais_pelis:
            num_pais.append(pais[1])

        top_pais = []
        for pais in id_pais:
            if pais[0] != 0:
                tupla = num_pais.count(pais[0]), pais[0]
                top_pais.append(tupla)

        top = sorted(top_pais, key=lambda x : x[0], reverse=True)
        top = top[0:5]

        print(plataforma)
        print(top)

        noms = []
        for pais in top:
            cur.execute(consulta_plataformes_nom_paisos, (pais[1], ))
            nom_pais = cur.fetchall()[0][0]
            noms.append(nom_pais)

        print(noms)

        quantitat_fig = [fila[0] for fila in top]
        fig, ax = plt.subplots(figsize=(14, 6))
        ax.barh(noms, quantitat_fig)
        plt.title("Països que han produït més contingut a la plataforma " + plataforma + ":")
        plt.show()

        num_pais.clear()

    tancar_connexio(conn, cur)

def mesos_mes_contingut(plataformes):
    conn = obrir_connexio()[0]
    cur = obrir_connexio()[1]

    num_data = []

    consulta_plataformes_data_series = "SELECT DISTINCT show_title, to_char(date_added, 'DD/MM/YYYY') FROM tvshows WHERE cataleg=%s"
    consulta_plataformes_data_pelis = "SELECT DISTINCT show_title, to_char(date_added, 'DD/MM/YYYY') FROM movies WHERE cataleg=%s"

    for plataforma in plataformes:
        cur.execute(consulta_plataformes_data_series, (plataforma, ))
        data_series = cur.fetchall()

        cur.execute(consulta_plataformes_data_pelis, (plataforma, ))
        data_pelis = cur.fetchall()

        for data in data_series:
            if data[1] != '01/01/0001':
                data_convert = datetime.strptime(str(data[1]), "%d/%m/%Y")
                num_data.append(data_convert.month)

        for data in data_pelis:
            if data[1] != '01/01/0001':
                data_convert = datetime.strptime(str(data[1]), "%d/%m/%Y")
                num_data.append(data_convert.month)

        top_mesos = []
        for mes in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]:
            tupla = num_data.count(mes), mes
            top_mesos.append(tupla)

        #print(top_mesos)

        top = sorted(top_mesos, key=lambda x: x[0], reverse=True)
        top = top[0:3]

        print(plataforma)
        print(top)

        mesos = ['Gener', 'Febrer', 'Març', 'Abril', 'Maig', 'Juny', 'Juliol', 'Agost', 'Setembre', 'Octubre', 'Novembre', 'Desembre']
        mesos_cataleg = []
        cont = 0
        while cont < len(top):
            mesos_cataleg.append(mesos[int(top[cont][1]) - 1])
            cont = cont + 1

        print(mesos_cataleg)
        print()

        quantitat_fig = [fila[0] for fila in top]
        fig, ax = plt.subplots(figsize=(14, 6))
        ax.barh(mesos_cataleg, quantitat_fig)
        plt.title("Top 3 mesos de l'any en que la plataforma " + plataforma + " puja més contingut:")
        plt.show()

        num_data.clear()

    tancar_connexio(conn, cur)

def pelis_valorades():
    conn = obrir_connexio()[0]
    cur = obrir_connexio()[1]

    num_rotten = []
    rotten_rating = []
    num_filmtv = []
    filmtv_rating = []

    consulta_valoracio_rotten = "SELECT show_title, tomatometer_rating from rottentomatoes order by show_title,tomatometer_rating desc limit 10"
    top10_rotten_rating = "SELECT distinct movies.show_title, cataleg, tomatometer_rating from movies inner join rottentomatoes on rottentomatoes.show_title = movies.show_title order by tomatometer_rating desc limit 10"
    consulta_valoracio_filmtv = "SELECT show_title, rating_avg from filmtv order by rating_avg desc limit 10"
    top10_filmtv_rating = "SELECT distinct movies.show_title, cataleg, rating_avg from movies inner join filmtv on filmtv.show_title = movies.show_title order by rating_avg desc limit 10"

    cur.execute(consulta_valoracio_rotten, ())
    rotten_valoracio = cur.fetchall()

    for rotten in rotten_valoracio:
        num_rotten.append(rotten[0])
        #print(rotten)

    rotten_valoracio = sorted(rotten_valoracio, key=lambda x: x[1], reverse=True)
    quantitat_fig = [int(fila[1]) for fila in rotten_valoracio]
    noms = [fila[0] for fila in rotten_valoracio]
    fig, ax = plt.subplots(figsize=(24, 8))
    ax.barh(noms, quantitat_fig)
    plt.title("Top 10 pel·licules més valorades a Rotten Tomatoes")
    plt.show()

    # print("------------------------------------------------------------")
    # print("     TOP 10 pel·licules més valorades a Rotten Tomatoes     ")
    # print("------------------------------------------------------------")
    # for i in num_rotten:
        # print(i)

    # print()

    #-----------------------------------------------------------------------------------------------------------------------
    cur.execute(top10_rotten_rating, ())
    rotten_top_movies = cur.fetchall()

    for rotten in rotten_top_movies:
        rotten_rating.append(rotten[0])
        #print(rotten)

    rotten_top_movies = sorted(rotten_top_movies, key=lambda x: x[1], reverse=True)
    quantitat_fig = [int(fila[2]) for fila in rotten_top_movies]
    noms = [str(fila[0] + " (" + fila[1] + ")") for fila in rotten_top_movies]
    fig, ax = plt.subplots(figsize=(30, 8))
    ax.barh(noms, quantitat_fig)
    plt.title("Top 10 pel·licules més valorades a Rotten Tomatoes que es troben a plataformes")
    plt.show()

    # print("-------------------------------------------------------------------------------------------------")
    # print("        TOP 10 pel·licules més valorades a Rotten Tomatoes que es troben a plataformes           ")
    # print("-------------------------------------------------------------------------------------------------")
    # for i in rotten_rating:
        # print(i)

    # print()

    # -----------------------------------------------------------------------------------------------------------------------
    cur.execute(consulta_valoracio_filmtv, ())
    filmtv_valoracio = cur.fetchall()

    for filmtv in filmtv_valoracio:
        num_filmtv.append(filmtv[0])
        #print(filmtv)

    filmtv_valoracio = sorted(filmtv_valoracio, key=lambda x: x[1], reverse=True)
    quantitat_fig = [int(fila[1]) for fila in filmtv_valoracio]
    noms = [fila[0] for fila in filmtv_valoracio]
    fig, ax = plt.subplots(figsize=(26, 8))
    ax.barh(noms, quantitat_fig)
    plt.title("Top 10 pel·licules més valorades a FilmTV")
    plt.show()

    # print("------------------------------------------------------------")
    # print("        TOP 10 pel·licules més valorades a FilmTV           ")
    # print("------------------------------------------------------------")
    # for i in num_filmtv:
        # print(i)

    # print()

    # -----------------------------------------------------------------------------------------------------------------------
    cur.execute(top10_filmtv_rating, ())
    filmtv_top_movies = cur.fetchall()

    for filmtv in filmtv_top_movies:
        filmtv_rating.append(filmtv[0])
        print(filmtv)

    filmtv_top_movies = sorted(filmtv_top_movies, key=lambda x: x[2], reverse=True)
    quantitat_fig = [int(fila[2]) for fila in filmtv_top_movies]
    noms = [str(fila[0] + " (" + fila[1] + ")") for fila in filmtv_top_movies]
    fig, ax = plt.subplots(figsize=(26, 8))
    ax.barh(noms, quantitat_fig)
    plt.title("Top 10 pel·licules més valorades a FilmTV que es troben a plataformes ")
    plt.show()

    # print("----------------------------------------------------------------------------------------")
    # print("        TOP 10 pel·licules més valorades a FilmTV que es troben a plataformes           ")
    # print("----------------------------------------------------------------------------------------")
    # for i in filmtv_rating:
        # print(i)

    # print()

    tancar_connexio(conn, cur)

def total_valorades(plataformes):
    conn = obrir_connexio()[0]
    cur = obrir_connexio()[1]

    consulta_plataformes_total_rotten = "SELECT count(distinct movies.show_title) from movies inner join rottentomatoes on rottentomatoes.show_title = movies.show_title and cataleg=%s"
    consulta_plataformes_total_filmtv = "SELECT count(distinct movies.show_title) from movies inner join filmtv on filmtv.show_title = movies.show_title and cataleg=%s"

    count_plataformes = []
    count_rotten = []
    count_filmtv = []

    for plataforma in plataformes:
        cur.execute(consulta_plataformes_total_rotten, (plataforma, ))
        total_rotten = cur.fetchall()

        cur.execute(consulta_plataformes_total_filmtv, (plataforma,))
        total_filmtv = cur.fetchall()

        num_rotten = total_rotten[0][0]
        num_filmtv = total_filmtv[0][0]

        count_plataformes.append(plataforma)
        count_rotten.append(num_rotten)
        count_filmtv.append(num_filmtv)

        print("Total contingut valorat de la plataforma '" + plataforma + "' a Rotten Tomatoes: " + str(num_rotten))
        print("Total contingut valorat de la plataforma '" + plataforma + "' a FilmTV: " + str(num_filmtv))
        print()

    plt.bar(count_plataformes, count_rotten, label='Rotten Tomatoes')
    plt.bar(count_plataformes, count_filmtv, label='FilmTV', bottom=count_rotten)
    plt.title("Contingut de cada plataforma de streaming amb valoració:")
    plt.legend()
    plt.show()

    tancar_connexio(conn, cur)

def decada_pelis():
    conn = obrir_connexio()[0]
    cur = obrir_connexio()[1]

    any1 = ['1890','1900','1910','1920','1930','1940','1950','1960','1970','1980','1990','2000','2010','2020']
    any2 = ['1899','1909','1919','1929','1939','1949','1959','1969','1979','1989','1999','2009','2019','2029']

    consulta_decada_filmtv = "SELECT distinct show_title, rating_avg from filmtv where realeaseyear between %s and %s"
    consulta_decada_rotten = "SELECT distinct show_title, tomatometer_rating from rottentomatoes where realeaseyear between %s and %s"

    decades_filmtv = []
    decades_rotten = []

    i = 0

    while i < len(any1):
        cur.execute(consulta_decada_filmtv, (any1[i], any2[i]), )
        ftv_decada = cur.fetchall()

        valoracions_filmtv_decada = 0
        for movie in ftv_decada:
            valoracions_filmtv_decada = valoracions_filmtv_decada + int(movie[1])

        print(valoracions_filmtv_decada/len(ftv_decada))

        tupla_filmtv = round(float(valoracions_filmtv_decada/len(ftv_decada)), 2), str(any1[i] + "-" + any2[i])
        decades_filmtv.append(tupla_filmtv)

        cur.execute(consulta_decada_rotten, (any1[i], any2[i]), )
        rotten_decada = cur.fetchall()

        valoracions_rotten_decada = 0
        for movie in rotten_decada:
            valoracions_rotten_decada = valoracions_rotten_decada + int(movie[1])

        if len(rotten_decada) == 0:
            mida_rotten = 1
        else:
            mida_rotten = len(rotten_decada)

        print(valoracions_rotten_decada / mida_rotten)

        tupla_rotten = round(float(valoracions_rotten_decada/mida_rotten), 2), str(any1[i] + "-" + any2[i])
        decades_rotten.append(tupla_rotten)

        i = i + 1

    print(decades_filmtv)
    print(decades_rotten)

    total_fig = [fila[0] for fila in decades_rotten]
    plataformes_fig = [fila[1] for fila in decades_rotten]
    fig, ax = plt.subplots(figsize=(18,7))
    ax.bar(plataformes_fig, total_fig)
    plt.title("Mitjana de valoracions de cada dècada a Rotten Tomatoes:")
    plt.show()

    total_fig = [fila[0] for fila in decades_filmtv]
    plataformes_fig = [fila[1] for fila in decades_filmtv]
    fig, ax = plt.subplots(figsize=(18,7))
    ax.bar(plataformes_fig, total_fig)
    plt.title("Mitjana de valoracions de cada dècada a FilmTV:")
    plt.show()

    # print(tupla)

    tancar_connexio(conn, cur)

def relacio_any_valoració():
    conn = obrir_connexio()[0]
    cur = obrir_connexio()[1]

    any = []
    mitja = []

    any_rotten = []
    mitja_rotten = []

    any_actual_filmtv='1897'
    ANY_MAX_FILMTV='2021'

    consulta_mitja_filmtv = "select avg(rating_avg) from filmtv where realeaseyear=%s"
    consulta_mitja_rotten = "select avg(tomatometer_rating) from rottentomatoes where realeaseyear=%s"

    while int(any_actual_filmtv) <= int(ANY_MAX_FILMTV):
        cur.execute(consulta_mitja_filmtv, (any_actual_filmtv, ))
        ftv_mitja = cur.fetchall()
        ftv_mitja = ftv_mitja[0][0]

        if (ftv_mitja != None):
            ftv_mitja = round(ftv_mitja, 2)
            any.append(any_actual_filmtv)
            mitja.append(ftv_mitja)

        cur.execute(consulta_mitja_rotten, (any_actual_filmtv,))
        rotten_mitja = cur.fetchall()
        rotten_mitja = rotten_mitja[0][0]

        if (rotten_mitja != None):
            rotten_mitja = round(rotten_mitja, 2)
            any_rotten.append(any_actual_filmtv)
            mitja_rotten.append(rotten_mitja)

        any_a = int(any_actual_filmtv)+1
        any_actual_filmtv=str(any_a)

    i=0
    print("FILMTV")
    while i < len(any):
        print("Any: " + any[i] + " , Mitja: " + str(mitja[i]))
        i=i+1

    print("---------------------")

    y=0
    print("ROTTEN")
    while y < len(any_rotten):
        print("Any: " + any_rotten[y] + " , Mitja: " + str(mitja_rotten[y]))
        y=y+1

    fig, ax = plt.subplots(figsize=(15, 6))
    ax.bar(any, mitja)
    plt.title("Mitjana de valoracions per any a FilmTV")
    plt.show()

    fig, ax = plt.subplots(figsize=(15, 6))
    ax.bar(any_rotten, mitja_rotten)
    plt.title("Mitjana de valoracions per any a Rotten Tomatoes")
    plt.show()

    # print(tupla)

    tancar_connexio(conn, cur)

def distribucio_valorades(plataformes):
    conn = obrir_connexio()[0]
    cur = obrir_connexio()[1]

    tupla = None

    MAX_FTV=0
    MIN_FTV=100
    platMAX_FTV=''
    platMIN_FTV=''

    MAX_RT = 0
    MIN_RT = 100
    platMAX_RT = ''
    platMIN_RT = ''

    rotten = []
    filmtv = []

    consulta_plataformes_rotten = "SELECT distinct avg(tomatometer_rating) from movies inner join rottentomatoes on rottentomatoes.show_title = movies.show_title and movies.cataleg=%s"
    consulta_plataformes_filmtv = "SELECT distinct avg(rating_avg) from movies inner join filmtv on filmtv.show_title = movies.show_title and movies.cataleg=%s"

    for plataforma in plataformes:
        cur.execute(consulta_plataformes_filmtv, (plataforma,))
        total_filmtv = cur.fetchall()

        num_filmtv = total_filmtv[0][0]
        num_filmtv = round(num_filmtv, 2)

        if num_filmtv > MAX_FTV:
            MAX_FTV=num_filmtv
            platMAX_FTV=plataforma

        if num_filmtv < MIN_FTV:
            MIN_FTV=num_filmtv
            platMIN_FTV=plataforma

        tupla_filmtv = plataforma, num_filmtv
        filmtv.append(tupla_filmtv)

        cur.execute(consulta_plataformes_rotten, (plataforma,))
        total_rotten = cur.fetchall()

        num_rotten = total_rotten[0][0]
        num_rotten = round(num_rotten, 2)

        if num_rotten > MAX_RT:
            MAX_RT=num_rotten
            platMAX_RT=plataforma

        if num_rotten < MIN_RT:
            MIN_RT=num_rotten
            platMIN_RT=plataforma

        tupla_rotten = plataforma, num_rotten
        rotten.append(tupla_rotten)


    print("FILMTV")
    print("Millors pelis " + platMAX_FTV + " amb una mitja de " + str(MAX_FTV))
    print("Pitjors pelis " + platMIN_FTV + " amb una mitja de " + str(MIN_FTV))
    print()

    print("ROTTEN TOMATOES")
    print("Millors pelis " + platMAX_RT + " amb una mitja de " + str(MAX_RT))
    print("Pitjors pelis " + platMIN_RT + " amb una mitja de " + str(MIN_RT))
    print()

    plataformes_fig = [fila[0] for fila in rotten]
    total_fig = [int(fila[1]) for fila in rotten]
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.bar(plataformes_fig, total_fig)
    plt.title("Distribució de valoracions de pel·lícules de Rotten Tomatoes a cada plataforma:")
    plt.show()

    plataformes_fig = [fila[0] for fila in filmtv]
    total_fig = [int(fila[1]) for fila in filmtv]
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.bar(plataformes_fig, total_fig)
    plt.title("Distribució de valoracions de pel·lícules de FilmTV a cada plataforma:")
    plt.show()

    tancar_connexio(conn, cur)

def distribucio_plataformes(plataformes):
    conn = obrir_connexio()[0]
    cur = obrir_connexio()[1]

    p1 = ['0', '10', '20', '30', '40', '50', '60', '70', '80', '90']
    p2 = ['10', '20', '30', '40', '50', '60', '70', '80', '90', '100']

    p3 = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']
    p4 = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10']

    valors_rotten = []
    valors_filmtv = []

    consulta_plataformes_rotten = "SELECT distinct count(tomatometer_rating) from movies inner join rottentomatoes on rottentomatoes.show_title = movies.show_title and movies.cataleg=%s where tomatometer_rating between %s and %s"
    consulta_plataformes_filmtv = "SELECT distinct count(rating_avg) from movies inner join filmtv on filmtv.show_title = movies.show_title and movies.cataleg=%s where rating_avg between %s and %s"

    print("-------------------------------------------------------------")
    print("                         ROTTEN                              ")
    print("-------------------------------------------------------------")
    for plataforma in plataformes:
        i = 0
        dist_rt = []

        while i < len(p1):
            cur.execute(consulta_plataformes_rotten, (plataforma, p1[i], p2[i]), )
            rotten_val = cur.fetchall()

            rotten_val = int(rotten_val[0][0])
            dist_rt.append(rotten_val)
            i = i + 1

        z = 0
        print("Plataforma " + plataforma)
        while z < len(dist_rt):
            print("Distribució entre " + p1[z] + " i " + p2[z] + " = " + str(dist_rt[z]) + " pel·lícules")
            tupla_rotten = "Distribució entre " + p1[z] + " i " + p2[z], str(dist_rt[z])
            valors_rotten.append(tupla_rotten)
            z = z + 1

        valors_fig = [fila[0] for fila in valors_rotten]
        total_fig = [int(fila[1]) for fila in valors_rotten]
        fig, ax = plt.subplots(figsize=(16, 7))
        ax.barh(valors_fig, total_fig)
        plt.title("Distribució de valoracions Rotten Tomatoes a la plataforma " + plataforma + ":")
        plt.show()

        valors_rotten.clear()

        print()

    print("-------------------------------------------------------------")
    print("                        FILMTV                               ")
    print("-------------------------------------------------------------")
    for plataforma in plataformes:
        i = 0
        dist_rt = []

        while i < len(p3):
            cur.execute(consulta_plataformes_filmtv, (plataforma, p3[i], p4[i]), )
            filmtv_val = cur.fetchall()

            filmtv_val = int(filmtv_val[0][0])
            dist_rt.append(filmtv_val)
            i = i + 1

        z = 0
        print("Plataforma " + plataforma)
        while z < len(dist_rt):
            print("Distribució entre " + p3[z] + " i " + p4[z] + " = " + str(dist_rt[z]) + " pel·lícules")
            tupla_filmtv = "Distribució entre " + p3[z] + " i " + p4[z], str(dist_rt[z])
            valors_filmtv.append(tupla_filmtv)
            z = z + 1

        valors_fig = [fila[0] for fila in valors_filmtv]
        total_fig = [int(fila[1]) for fila in valors_filmtv]
        fig, ax = plt.subplots(figsize=(16, 7))
        ax.barh(valors_fig, total_fig)
        plt.title("Distribució de valoracions FilmTV a la plataforma " + plataforma + ":")
        plt.show()

        valors_filmtv.clear()

        print()

    tancar_connexio(conn, cur)

def distribucio_anys_plataformes(plataformes):
    conn = obrir_connexio()[0]
    cur = obrir_connexio()[1]

    consulta_anys_series = "SELECT DISTINCT show_title, release_year FROM tvshows WHERE cataleg=%s ORDER BY release_year"
    consulta_anys_pelicules = "SELECT DISTINCT show_title, release_year FROM movies WHERE cataleg=%s ORDER BY release_year"

    anys = []

    for plataforma in plataformes:
        cur.execute(consulta_anys_series, (plataforma, ))
        tvshows = cur.fetchall()

        cur.execute(consulta_anys_pelicules, (plataforma, ))
        movies = cur.fetchall()

        for tvshow in tvshows:
            anys.append(tvshow[1])
            #print(tvshow[1])

        for movie in movies:
            anys.append(movie[1])
            #print(movie[1])

        any_inicial = min(anys)
        any_final = max(anys)

        cont = any_inicial
        anys_fig = []
        quantitat_fig = []

        while cont < (any_final + 1):
            print("Any: " + str(cont) + " Total: " + str(anys.count(cont)))
            anys_fig.append(cont)
            quantitat_fig.append(anys.count(cont))
            cont = cont + 1

        fig, ax = plt.subplots(figsize=(15, 6))
        ax.barh(anys_fig, quantitat_fig)
        plt.title("Distribució per anys del contingut de " + plataforma + ":")
        plt.show()

        anys.clear()

    tancar_connexio(conn, cur)
    
def pelicules_amb_mes_vots():
    conn = obrir_connexio()[0]
    cur = obrir_connexio()[1]

    consulta_rotten = "SELECT show_title, rotten_critics, tomatometer_rating FROM rottentomatoes ORDER BY rotten_critics DESC"
    consulta_rotten2 = "SELECT show_title, fresh_critics, tomatometer_rating FROM rottentomatoes ORDER BY fresh_critics DESC"
    consulta_filmtv = "SELECT show_title, votes_count, rating_avg FROM filmtv ORDER BY votes_count DESC"
    consulta_filmtv2 = "SELECT show_title, rating_avg FROM filmtv ORDER BY rating_avg DESC"

    cur.execute(consulta_rotten, ())
    rotten = cur.fetchall()
    rotten = rotten[0:10]

    pelicules_fig = [fila[0] for fila in rotten]
    total_fig = [int(fila[1]) for fila in rotten]
    fig, ax = plt.subplots(figsize=(40, 8))
    ax.barh(pelicules_fig, total_fig)
    plt.title("Top 10 pel·lícules amb més crítiques negatives:")
    plt.show()

    pelicules_fig = [fila[0] for fila in rotten]
    total_fig = [int(fila[2]) for fila in rotten]
    fig, ax = plt.subplots(figsize=(40, 8))
    ax.barh(pelicules_fig, total_fig)
    plt.title("Valoracions del top 10 pel·lícules amb més crítiques negatives:")
    plt.show()

    cur.execute(consulta_rotten2, ())
    rotten2 = cur.fetchall()
    rotten2 = rotten2[0:10]

    pelicules_fig = [fila[0] for fila in rotten2]
    total_fig = [int(fila[1]) for fila in rotten2]
    fig, ax = plt.subplots(figsize=(40, 8))
    ax.barh(pelicules_fig, total_fig)
    plt.title("Top 10 pel·lícules amb més crítiques positives:")
    plt.show()

    pelicules_fig = [fila[0] for fila in rotten2]
    total_fig = [int(fila[2]) for fila in rotten2]
    fig, ax = plt.subplots(figsize=(40, 8))
    ax.barh(pelicules_fig, total_fig)
    plt.title("Valoracions del top 10 pel·lícules amb més crítiques positives:")
    plt.show()

    cur.execute(consulta_filmtv, ())
    filmtv = cur.fetchall()
    filmtv = filmtv[0:10]

    pelicules_fig = [fila[0] for fila in filmtv]
    total_fig = [int(fila[1]) for fila in filmtv]
    fig, ax = plt.subplots(figsize=(40, 8))
    ax.barh(pelicules_fig, total_fig)
    plt.title("Top 10 pel·lícules amb més vots:")
    plt.show()

    pelicules_fig = [fila[0] for fila in filmtv]
    total_fig = [int(fila[2]) for fila in filmtv]
    fig, ax = plt.subplots(figsize=(40, 8))
    ax.barh(pelicules_fig, total_fig)
    plt.title("Valoracions del top 10 pel·lícules amb més vots:")
    plt.show()

    cur.execute(consulta_filmtv2, ())
    filmtv2 = cur.fetchall()
    filmtv2 = filmtv2[0:10]

    pelicules_fig = [fila[0] for fila in filmtv2]
    total_fig = [int(fila[1]) for fila in filmtv2]
    fig, ax = plt.subplots(figsize=(40, 8))
    ax.barh(pelicules_fig, total_fig)
    plt.title("Top 10 pel·lícules amb millors valoracions:")
    plt.show()

    print()

    tancar_connexio(conn, cur)

if __name__ == '__main__':

    plataformes = ["Netflix", "AmazonPrime", "DisneyPlus", "Hulu"]
    
    # Quina plataforma de streaming té més contingut? I la que menys?
    # print("Quina plataforma de streaming té més contingut? I la que menys?")
    # contingut_plataformes(plataformes)
    print()
    
    # Quina és la distribució entre pel·lícules i sèries de cada plataforma de streaming? Si sou més aficionats a les sèries, quina preferirieu?
    # print("Quina és la distribució entre pel·lícules i sèries de cada plataforma de streaming? Si sou més aficionats a les sèries, quina preferirieu?")
    # contingut_separat_plataformes(plataformes)
    print()

    # Quins són els directors/es més repetits? I els actors/actrius?
    # print("Quins són els directors/es més repetits? I els actors/actrius?")
    # directors_mes_repetits()
    # actors_mes_repetits()
    print()
        
    # Quina és la distribució de duració de les pel·licules a cada plataforma de streaming? I de les sèries?
    # print("Quina és la distribució de duració de les pel·licules a cada plataforma de streaming? I de les sèries?")
    # duracio_pelis(plataformes)
    # duracio_series(plataformes)
    print()
    
    # Quins són els gèneres més populars a cada plataforma de streaming?
    # print("Quins són els gèneres més populars a cada plataforma de streaming?")
    # generes_populars(plataformes)
    print()
    
    # Quins són els països que han produït més contingut? És similar entre les diverses plataformes de streaming?
    # print("Quins són els països que han produït més contingut? És similar entre les diverses plataformes de streaming?")
    # països_mes_contingut()
    # països_mes_contingut_plataformes(plataformes)
    print()
    
    # Quins són els mesos de l'any en que les plataformes de streaming afegeixen més nou contingut?
    # print("Quins són els mesos de l'any en que les plataformes de streaming afegeixen més nou contingut?")
    # mesos_mes_contingut(plataformes)
    print()
    
    # Quines són les pel·lícules més valorades a Rotten Tomatoes? I a FilmTV? Es troben en plataformes de streaming?
    # print("Quines són les pel·lícules més valorades a Rotten Tomatoes? I a FilmTV? Es troben en plataformes de streaming?")
    # pelis_valorades()
    print()
    
    # Quant contingut de cada plataforma de streaming té alguna valoració (a Rotten Tomatoes o a FilmTV)?
    # print("Quant contingut de cada plataforma de streaming té alguna vasloració (a Rotten Tomatoes o a FilmTV)?")
    # total_valorades(plataformes)
    print()
    
    # Quina dècada va tindre millors pel·lícules?
    # print("Quina dècada va tindre millors pel·lícules?")
    # decada_pelis()
    print()
    
    # Existeix una relació entre l'any de les pel·licules i la seva valoració? Podem dir que les pel·licules noves tenen millors valoracions que les més antigues?
    # print("Existeix una relació entre l'any de les pel·licules i la seva valoració? Podem dir que les pel·licules noves tenen millors valoracions que les més antigues?")
    # relacio_any_valoració()
    print()
    
    # Quina plataforma té millors pel·lícules? I pitjors? Quina és la distribució de valoracions de pel·licules en cada plataforma de streaming?
    # print("Quina plataforma té millors pel·lícules? I pitjors? Quina és la distribució de valoracions de pel·licules en cada plataforma de streaming?")
    # distribucio_valorades(plataformes)
    # distribucio_plataformes(plataformes)
    print()
    
    # Quina és la distribució per anys del contingut de cada plataforma de streaming? Quina és la que té un contingut més nou? I la que té un contingut més vell? La tendència és manté segons si el contingut són pel·lícules o sèries?
    # print("Quina és la distribució per anys del contingut de cada plataforma de streaming? Quina és la que té un contingut més nou? I la que té un contingut més vell? La tendència és manté segons si el contingut són pel·lícules o sèries?")
    # distribucio_anys_plataformes(plataformes)
    print()
    
    # Les pel·lícules amb més vots/crítiques són les que tenen millors valoracions?
    # print("Les pel·lícules amb més vots/crítiques són les que tenen millors valoracions?")
    # pelicules_amb_mes_vots()
    print()
    
    
    