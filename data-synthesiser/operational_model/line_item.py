from trumania.core.random_generators import SequencialGenerator, FakerGenerator
from .synthesizer import Synthesizer
import random


class LineItemSynthesizer(Synthesizer):

    def __init__(self, env, per_purchase_ratio):
        super(LineItemSynthesizer, self).__init__(env)
        self.per_purchase_ratio = per_purchase_ratio

    def synthesize_bucket(self, purchase_fks, product_fks, start=0):
        idx_gen = SequencialGenerator(prefix="LI_", start=start)
        purchases_in_bucket = []
        products_in_bucket = []
        for purchase in purchase_fks:
            size = random.randint(1, self.per_purchase_ratio)
            purchases_in_bucket.extend([purchase for _ in range(size)])
            products_in_bucket.extend(random.sample(product_fks, size))
        line_item_formula = self.dataset.create_population(name="line_item_%d" % start,
                                                                  size=len(purchases_in_bucket), ids_gen=idx_gen)
        df = line_item_formula.to_dataframe()
        df["purchase_id"] = purchases_in_bucket
        df["product_id"] = products_in_bucket
        return df

    def synthesize_and_push(self, purchase_fks, product_fks, conn, if_exists="replace", start=0):
        return self.push_to_db(self.synthesize_bucket(purchase_fks, product_fks, start), conn, if_exists)

    @staticmethod
    def push_to_db(line_item_data_frame, conn, if_exists="replace"):
        line_item_data_frame.to_sql("line_item", conn, if_exists=if_exists, index_label="line_item_id")
        return len(line_item_data_frame)
