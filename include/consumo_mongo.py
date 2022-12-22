import pymongo
from bson.json_util import dumps

def consumo_mongo():
   
    # String de conexão com o mongoDB online
    string_connection = "mongodb+srv://sylviopucminas:_Kz63V29-j_QE3Z@cluster0.gue70sx.mongodb.net/test"

    # DataBase_name a ser conectada para consumo:
    database_name = "sample_airbnb"
    collection_name = "listingsAndReviews"

    # conectando ao MongoDB Server
    client = pymongo.MongoClient(string_connection)

    # Acessando a collection listingsAndReviews, dentro do banco do sample_airbnb.  
    collection = client[database_name][collection_name]

    # Criando um instancia de cursor, realizando uma query no documento a partir do comando "find()"  
    cursor = collection.find()

    # Transformando o cursor em uma lista de dicionarios
    list_cur = list(cursor)

    # Convertendo para uma estrutura Json
    json_data = dumps(list_cur)

    # Escrevendo os dados em um arquivo Json
    with open('data.json', 'w') as file:
        file.write(json_data)
