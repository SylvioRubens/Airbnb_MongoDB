import json
import pandas as pd
import sqlalchemy as db

trusted_tbl = "airbnb_trusted"
refined_tbl = "airbnb_refined"

config = {
            'host': 'host.docker.internal',
            'port': 3309,
            'user': 'root',
            'password': 'root',
            'database': 'airbnb'
          }

db_user = config.get('user')
db_pwd = config.get('password')
db_host = config.get('host')
db_port = config.get('port')
db_name = config.get('database')

# specify connection string
connection_str = f'mysql+pymysql://{db_user}:{db_pwd}@{db_host}:{db_port}/{db_name}?charset=utf8mb4'

conn = db.create_engine(connection_str)

def read_json():

    with open('data.json') as data_file:
        data = json.load(data_file)

    normalized_data = pd.json_normalize(data)

    return normalized_data

def populate_airbnb_trusted():

    data = read_json()
    data = data.applymap(str)
    data = data.drop(['reviews'], axis=1)

    try:

        command1 = """DROP TABLE IF EXISTS `airbnb_trusted`;"""

        command2 =   """
                            
                            CREATE TABLE `airbnb_trusted` 
                            (
                            
                              `_id`											TEXT COLLATE utf8mb4_unicode_ci,
                              `listing_url`									TEXT COLLATE utf8mb4_unicode_ci,
                              `name`										TEXT COLLATE utf8mb4_unicode_ci,
                              `summary`										TEXT COLLATE utf8mb4_unicode_ci,
                              `space`										TEXT COLLATE utf8mb4_unicode_ci,
                              `description`									TEXT COLLATE utf8mb4_unicode_ci,
                              `neighborhood_overview`						TEXT COLLATE utf8mb4_unicode_ci,
                              `notes`										TEXT COLLATE utf8mb4_unicode_ci,
                              `transit`										TEXT COLLATE utf8mb4_unicode_ci,
                              `access`										TEXT COLLATE utf8mb4_unicode_ci,
                              `interaction`									TEXT COLLATE utf8mb4_unicode_ci,
                              `house_rules`									TEXT COLLATE utf8mb4_unicode_ci,
                              `property_type`								TEXT COLLATE utf8mb4_unicode_ci,
                              `room_type`									TEXT COLLATE utf8mb4_unicode_ci,
                              `bed_type`									TEXT COLLATE utf8mb4_unicode_ci,
                              `minimum_nights`								TEXT COLLATE utf8mb4_unicode_ci,
                              `maximum_nights`								TEXT COLLATE utf8mb4_unicode_ci,
                              `cancellation_policy`							TEXT COLLATE utf8mb4_unicode_ci,
                              `accommodates`								TEXT COLLATE utf8mb4_unicode_ci,
                              `bedrooms`									TEXT COLLATE utf8mb4_unicode_ci,
                              `beds`										TEXT COLLATE utf8mb4_unicode_ci,
                              `number_of_reviews`							TEXT COLLATE utf8mb4_unicode_ci,
                              `amenities`									TEXT COLLATE utf8mb4_unicode_ci,
                              `last_scraped.$date`							TEXT COLLATE utf8mb4_unicode_ci,
                              `calendar_last_scraped.$date`					TEXT COLLATE utf8mb4_unicode_ci,
                              `first_review.$date`							TEXT COLLATE utf8mb4_unicode_ci,
                              `last_review.$date`							TEXT COLLATE utf8mb4_unicode_ci,
                              `bathrooms.$numberDecimal`					TEXT COLLATE utf8mb4_unicode_ci,
                              `price.$numberDecimal`						TEXT COLLATE utf8mb4_unicode_ci,
                              `security_deposit.$numberDecimal`				TEXT COLLATE utf8mb4_unicode_ci,
                              `cleaning_fee.$numberDecimal`					TEXT COLLATE utf8mb4_unicode_ci,
                              `extra_people.$numberDecimal`					TEXT COLLATE utf8mb4_unicode_ci,
                              `guests_included.$numberDecimal`				TEXT COLLATE utf8mb4_unicode_ci,
                              `images.thumbnail_url`						TEXT COLLATE utf8mb4_unicode_ci,
                              `images.medium_url`							TEXT COLLATE utf8mb4_unicode_ci,
                              `images.picture_url`							TEXT COLLATE utf8mb4_unicode_ci,
                              `images.xl_picture_url`						TEXT COLLATE utf8mb4_unicode_ci,
                              `host.host_id`								TEXT COLLATE utf8mb4_unicode_ci,
                              `host.host_url`								TEXT COLLATE utf8mb4_unicode_ci,
                              `host.host_name`								TEXT COLLATE utf8mb4_unicode_ci,
                              `host.host_location`							TEXT COLLATE utf8mb4_unicode_ci,
                              `host.host_about`								TEXT COLLATE utf8mb4_unicode_ci,
                              `host.host_response_time`						TEXT COLLATE utf8mb4_unicode_ci,
                              `host.host_thumbnail_url`						TEXT COLLATE utf8mb4_unicode_ci,
                              `host.host_picture_url`						TEXT COLLATE utf8mb4_unicode_ci,
                              `host.host_neighbourhood`						TEXT COLLATE utf8mb4_unicode_ci,
                              `host.host_response_rate`						TEXT COLLATE utf8mb4_unicode_ci,
                              `host.host_is_superhost`						TEXT COLLATE utf8mb4_unicode_ci,
                              `host.host_has_profile_pic`					TEXT COLLATE utf8mb4_unicode_ci,
                              `host.host_identity_verified`					TEXT COLLATE utf8mb4_unicode_ci,
                              `host.host_listings_count`					TEXT COLLATE utf8mb4_unicode_ci,
                              `host.host_total_listings_count`				TEXT COLLATE utf8mb4_unicode_ci,
                              `host.host_verifications`						TEXT COLLATE utf8mb4_unicode_ci,
                              `address.street`								TEXT COLLATE utf8mb4_unicode_ci,
                              `address.suburb`								TEXT COLLATE utf8mb4_unicode_ci,
                              `address.government_area`						TEXT COLLATE utf8mb4_unicode_ci,
                              `address.market`								TEXT COLLATE utf8mb4_unicode_ci,
                              `address.country`								TEXT COLLATE utf8mb4_unicode_ci,
                              `address.country_code`						TEXT COLLATE utf8mb4_unicode_ci,
                              `address.location.type`						TEXT COLLATE utf8mb4_unicode_ci,
                              `address.location.coordinates`				TEXT COLLATE utf8mb4_unicode_ci,
                              `address.location.is_location_exact`			TEXT COLLATE utf8mb4_unicode_ci,
                              `availability.availability_30`				TEXT COLLATE utf8mb4_unicode_ci,
                              `availability.availability_60`				TEXT COLLATE utf8mb4_unicode_ci,
                              `availability.availability_90`				TEXT COLLATE utf8mb4_unicode_ci,
                              `availability.availability_365`				TEXT COLLATE utf8mb4_unicode_ci,
                              `review_scores.review_scores_accuracy`		TEXT COLLATE utf8mb4_unicode_ci,
                              `review_scores.review_scores_cleanliness`		TEXT COLLATE utf8mb4_unicode_ci,
                              `review_scores.review_scores_checkin`			TEXT COLLATE utf8mb4_unicode_ci,
                              `review_scores.review_scores_communication`	TEXT COLLATE utf8mb4_unicode_ci,
                              `review_scores.review_scores_location`		TEXT COLLATE utf8mb4_unicode_ci,
                              `review_scores.review_scores_value`			TEXT COLLATE utf8mb4_unicode_ci,
                              `review_scores.review_scores_rating`			TEXT COLLATE utf8mb4_unicode_ci,
                              `weekly_price.$numberDecimal`					TEXT COLLATE utf8mb4_unicode_ci,
                              `monthly_price.$numberDecimal`				TEXT COLLATE utf8mb4_unicode_ci,
                              `reviews_per_month`							TEXT COLLATE utf8mb4_unicode_ci
                            
                            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
                    
                    """

        # Deletar tabela no bd.
        conn.execute(command1)

        # Alterar a codificação de texto.
        conn.execute(command2)

        # Salvar valores no banco de dados.
        data.to_sql(name=trusted_tbl, con=conn, if_exists='append', index=False)
        print("values entered into the database.")

    except Exception as erro:
        raise(erro)

def populate_airbnb_refined():

    try:

        command1 = """DROP TABLE IF EXISTS `airbnb_refined`;"""

        # Excluindo tabela `airbnb_refined` se existir.
        conn.execute(command1)

        command2 =  """
                        
                        CREATE 
                        TABLE	`airbnb_refined` AS
                                SELECT 
                                        `airbnb_trusted`.name																					AS `name`
                                    ,	`airbnb_trusted`.`price.$numberDecimal`																	AS `price`
                                    ,	SUBSTRING_INDEX(`airbnb_trusted`.`address.street`, ',', 1) 												AS `city`
                                    ,	SUBSTRING_INDEX(SUBSTRING_INDEX(`airbnb_trusted`.`address.street`, ',', 2), ',', -1) 	                AS `state`
                                    ,	`airbnb_trusted`.`address.country`																		AS 	`country`
                                    ,	`airbnb_trusted`.`address.market`																		AS `market`
                                    ,	`airbnb_trusted`.`review_scores.review_scores_rating`													AS `rating`
                                    ,	`airbnb_trusted`.`review_scores.review_scores_value`													AS `scores`
                                    ,	`airbnb_trusted`.`review_scores.review_scores_accuracy`													AS `accuracy`
                                    ,	`airbnb_trusted`.`review_scores.review_scores_cleanliness`												AS `cleanliness`
                                    ,	`airbnb_trusted`.`review_scores.review_scores_checkin`													AS `checkin`
                                    ,	`airbnb_trusted`.`review_scores.review_scores_communication`											AS `communication`
                                    ,	`airbnb_trusted`.`review_scores.review_scores_value`													AS `value`
                                    ,	`airbnb_trusted`.`number_of_reviews`																	AS `reviews`
                                    
                                FROM	`airbnb`.`airbnb_trusted`;
                    
                    """

        # Criando a zona refined.
        conn.execute(command2)
        print("created refined zone")

    except Exception as erro:
        raise(erro)

def close_connection():
    if conn.dispose() is None:
        print("Close connection.")
