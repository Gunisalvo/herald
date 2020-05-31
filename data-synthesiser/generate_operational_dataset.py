import random
from trumania.core import circus
from trumania.core.random_generators import SequencialGenerator, FakerGenerator, NumpyRandomGenerator
import pandas as pd
from sqlalchemy import create_engine


operational_dataset = circus.Circus(
    name="herald",
    master_seed=171,
    start=pd.Timestamp("01 Jan 2020 00:00"),
    step_duration=pd.Timedelta("3m"))


batch_size = 100


#users
def synthesize_user_bucket(size, missing_social_media_ratio, start=0):
    idx_gen = SequencialGenerator(prefix="USR_", start=start)
    age_gen = NumpyRandomGenerator(
        method="gamma",
        shape=15,
        scale=3,
        seed=next(operational_dataset.seeder)).map(f=lambda x: int(x))
    name_gen = FakerGenerator(method="name", seed=next(operational_dataset.seeder))
    social_gen = FakerGenerator(method="ean", seed=next(operational_dataset.seeder))
    user_formula = operational_dataset.create_population(name="user_%d" % start, size=size, ids_gen=idx_gen)
    user_formula.create_attribute("full_name", init_gen=name_gen)
    user_formula.create_attribute("age", init_gen=age_gen)
    user_formula.create_attribute("social", init_gen=social_gen)
    df = user_formula.to_dataframe()

    df.loc[
        df.sample(frac=missing_social_media_ratio).index,
        "social"
    ] = None

    return df


### address
def synthesize_addresses_bucket(user_fks, start=0):
    idx_gen = SequencialGenerator(prefix="ADDR_", start=start)
    str_gen = FakerGenerator(method="street_name", seed=next(operational_dataset.seeder))
    num_gen = FakerGenerator(method="building_number", seed=next(operational_dataset.seeder))
    city_gen = FakerGenerator(method="city", seed=next(operational_dataset.seeder))
    country_gen = FakerGenerator(method="bank_country", seed=next(operational_dataset.seeder))
    addr_formula = operational_dataset.create_population(name="address_%d" % start, size=len(user_fks), ids_gen=idx_gen)
    addr_formula.create_attribute("street", init_gen=str_gen)
    addr_formula.create_attribute("number", init_gen=num_gen)
    addr_formula.create_attribute("city", init_gen=city_gen)
    addr_formula.create_attribute("county", init_gen=country_gen)
    df = addr_formula.to_dataframe()
    df["user_id"] = user_fks
    return df


### paymentMethod
def synthesize_pay_type_bucket(user_fks, pay_type_per_user_ratio, start=0):
    idx_gen = SequencialGenerator(prefix="PAY_", start=start)
    size = random.randint(len(user_fks), len(user_fks) * pay_type_per_user_ratio)
    pay_type_formula = operational_dataset.create_population(name="pay_type_%d" % start,
                                                             size=size,
                                                             ids_gen=idx_gen)

    df = pay_type_formula.to_dataframe()

    user_seed = []
    for _ in range(pay_type_per_user_ratio):
        user_seed.extend(user_fks)

    df["user_id"] = random.sample(user_seed, len(df))
    return df


### purchase
def synthesize_purchase_bucket(pay_type_fk, per_pay_type_ratio, start=0):
    idx_gen = SequencialGenerator(prefix="PUR_", start=start)
    kyc_gen = FakerGenerator(method="ean", seed=next(operational_dataset.seeder))
    size = random.randint(len(pay_type_fk), len(pay_type_fk) * per_pay_type_ratio)
    purchase_formula = operational_dataset.create_population(name="purchase_%d" % start,
                                                             size=size,
                                                             ids_gen=idx_gen)
    purchase_formula.create_attribute("kyc", init_gen=kyc_gen)
    df = purchase_formula.to_dataframe()

    pay_type_seed = []
    for _ in range(per_pay_type_ratio):
        pay_type_seed.extend(pay_type_fk)

    df["pay_type_id"] = random.sample(pay_type_seed, len(df))
    return df


### product
def synthesize_products(size, start=0):
    idx_gen = SequencialGenerator(prefix="PROD_", start=start)
    prod_name_gen = FakerGenerator(method="sentence",
                                   nb_words=2,
                                   seed=next(operational_dataset.seeder)).map(f=lambda x: x[:-1])
    product_formula = operational_dataset.create_population(name="product_%d" % start, size=size, ids_gen=idx_gen)
    product_formula.create_attribute("name", init_gen=prod_name_gen)
    return product_formula.to_dataframe()


## line item
def synthesize_line_items_bucket(product_fk, purchase_fk, per_purchase_ratio, start=0):
    idx_gen = SequencialGenerator(prefix="LI_", start=start)
    purchases_in_bucket = []
    products_in_bucket = []
    for purchase in purchase_fk:
        size = random.randint(1, per_purchase_ratio)
        purchases_in_bucket.extend([purchase for _ in range(size)])
        products_in_bucket.extend(random.sample(product_fk, size))
    line_item_formula = operational_dataset.create_population(name="line_item_%d" % start, size=len(purchases_in_bucket), ids_gen=idx_gen)
    df = line_item_formula.to_dataframe()
    df["purchase_id"] = purchases_in_bucket
    df["product_id"] = products_in_bucket
    return df


### social
def synthesize_social_media_info(social_media_links, start=0):
    idx_gen = SequencialGenerator(prefix="SM_", start=start)
    size = len(social_media_links)
    social_media_info_formula = operational_dataset.create_population(name="social_media_info_%d" % start,
                                                             size=size,
                                                             ids_gen=idx_gen)

    df = social_media_info_formula.to_dataframe()
    df["social_link"] = social_media_links

    return df


### kyc
def synthesize_kyc_info(kyc_evaluation_links, start=0):
    idx_gen = SequencialGenerator(prefix="KYC_", start=start)
    size = len(kyc_evaluation_links)
    social_media_info_formula = operational_dataset.create_population(name="kyc_%d" % start,
                                                                      size=size,
                                                                      ids_gen=idx_gen)

    df = social_media_info_formula.to_dataframe()
    df["kyc_link"] = kyc_evaluation_links

    return df


def synthetize_data(n_of_users, n_of_products, conn_string="postgresql://postgres:opDB@localhost:15432"):
    engine = create_engine(conn_string)
    conn = engine.connect()

    if n_of_users % batch_size == 0:
        n_iterations = n_of_users // batch_size
    else:
        n_iterations = (n_of_users // batch_size) + 1

    address_count = 0
    pay_type_count = 0
    purchase_count = 0
    line_item_count = 0
    social_link_count = 0
    kyc_link_count = 0

    print("generating products")
    product_table = synthesize_products(n_of_products)
    product_table.to_sql("product", conn, if_exists="replace", index_label="product_id")
    product_fk = product_table.index.to_list()

    for i in range(n_iterations):
        print("generating bucket %d" % (i + 1))
        if i == 0:
            exists_command = "replace"
        else:
            exists_command = "append"

        if n_of_users - ((i+1) * batch_size) > 0:
            current_bucket_size = batch_size
        else:
            current_bucket_size = n_of_users - (i * batch_size)

        user_table = synthesize_user_bucket(current_bucket_size, 0.2, start=i * batch_size)
        user_table.to_sql("user", conn, if_exists=exists_command, index_label="user_id")
        user_fk = user_table.index.values
        social_links = user_table[(user_table["social"].notnull())]["social"].to_list()

        address_tabe = synthesize_addresses_bucket(user_fk, start=address_count)
        address_count += len(address_tabe)
        address_tabe.to_sql("address", conn, if_exists=exists_command, index_label="address_id")

        pay_type_table = synthesize_pay_type_bucket(user_fk, 3, start= pay_type_count)
        pay_type_count += len(pay_type_table)
        pay_type_table.to_sql("payment_type", conn, if_exists=exists_command, index_label="pay_type_id")
        pay_type_fk = pay_type_table.index.to_list()

        purchase_table = synthesize_purchase_bucket(pay_type_fk, 5, start=purchase_count)
        purchase_count += len(purchase_table)
        purchase_table.to_sql("purchase", conn, if_exists=exists_command, index_label="purchase_id")
        purchase_fk = purchase_table.index.to_list()
        kyc_links = purchase_table["kyc"].to_list()

        line_item_table = synthesize_line_items_bucket(product_fk, purchase_fk, 10, start=line_item_count)
        line_item_count += len(line_item_table)
        line_item_table.to_sql("line_item", conn, if_exists=exists_command, index_label="line_item_id")

        social_media_info_table = synthesize_social_media_info(social_links, start=social_link_count)
        social_link_count += len(social_media_info_table)
        social_media_info_table.to_sql("social_media", conn, if_exists=exists_command, index_label="sm_id")

        kyc_info_table = synthesize_kyc_info(kyc_links, start=kyc_link_count)
        kyc_link_count += len(kyc_info_table)
        kyc_info_table.to_sql("kyc", conn, if_exists=exists_command, index_label="kyc_id")

    conn.close()


synthetize_data(n_of_users=10000, n_of_products=100)