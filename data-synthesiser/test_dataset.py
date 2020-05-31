import configparser

from connector.sql_connector import get_connection
from logs.stdout import logger

from operational_model.address import AddressSynthesizer
from operational_model.kyc import KycSynthesizer
from operational_model.line_item import LineItemSynthesizer
from operational_model.pay_type import PayTypeSynthesizer
from operational_model.purchase import PurchaseSynthesizer
from operational_model.product import ProductSynthesizer
from operational_model.social_media import SocialMediaSynthesizer
from operational_model.user import UserSynthesizer


def generate(environment="local"):
    config = configparser.ConfigParser()
    config.read("config.ini")
    batch_size = config[environment].getint("batchSize")
    user_count = config[environment].getint("userCount")
    product_count = config[environment].getint("productCount")

    logger().info("Generating Test Data: %d users, %d products - %d batch size" % (user_count, product_count, batch_size))

    sql_connection = get_connection(environment)
    if user_count % batch_size == 0:
        n_iterations = user_count // batch_size
    else:
        n_iterations = (user_count // batch_size) + 1

    address_count = 0
    pay_type_count = 0
    purchase_count = 0
    line_item_count = 0
    social_link_count = 0
    kyc_link_count = 0

    logger().info("- Products ...")
    _, product_fks = ProductSynthesizer(environment)\
        .synthesize_and_push(product_count, sql_connection)

    for i in range(n_iterations):
        logger().info("- Bucket %d ..." % (i + 1))
        if i == 0:
            exists_command = "replace"
        else:
            exists_command = "append"

        if user_count - ((i+1) * batch_size) > 0:
            current_bucket_size = batch_size
        else:
            current_bucket_size = user_count - (i * batch_size)

        _, user_fks, social_links = UserSynthesizer(environment, missing_social_media_ratio=0.2)\
            .synthesize_and_push(current_bucket_size, sql_connection, exists_command)

        addr_generated = AddressSynthesizer(environment)\
            .synthesize_and_push(user_fks, sql_connection, if_exists=exists_command, start=address_count)
        address_count += addr_generated

        pay_type_generated, pay_type_fks = PayTypeSynthesizer(environment, pay_type_per_user_ratio=3)\
            .synthesize_and_push(user_fks, sql_connection, if_exists=exists_command, start=pay_type_count)
        pay_type_count += pay_type_generated

        purchases_generated, purchase_fks, kyc_links = PurchaseSynthesizer(environment, per_pay_type_ratio=5)\
            .synthesize_and_push(pay_type_fks, sql_connection, if_exists=exists_command, start=purchase_count)
        purchase_count += purchases_generated

        line_items_generated = LineItemSynthesizer(environment, per_purchase_ratio=10)\
            .synthesize_and_push(product_fks, purchase_fks, sql_connection, start=line_item_count)
        line_item_count += line_items_generated

        social_media_info_generated = SocialMediaSynthesizer(environment)\
            .synthesize_and_push(social_links, sql_connection, if_exists=exists_command, start=social_link_count)
        social_link_count += social_media_info_generated

        kyc_info_generated = KycSynthesizer(environment)\
            .synthesize_and_push(kyc_links, sql_connection, if_exists=exists_command, start=kyc_link_count)
        kyc_link_count += kyc_info_generated

    sql_connection.close()


if __name__ == "__main__":
    generate("local")