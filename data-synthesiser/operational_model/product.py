from trumania.core.random_generators import SequencialGenerator, FakerGenerator
from .synthesizer import Synthesizer


class ProductSynthesizer(Synthesizer):

    def __init__(self, env):
        super(ProductSynthesizer, self).__init__(env)
        self.prod_name_gen = FakerGenerator(method="sentence",
                                       nb_words=2,
                                       seed=next(self.dataset.seeder)).map(f=lambda x: x[:-1])

    def synthesize_bucket(self, size, start=0):
        idx_gen = SequencialGenerator(prefix="PROD_", start=start)
        product_formula = self.dataset.create_population(name="product_%d" % start, size=size, ids_gen=idx_gen)
        product_formula.create_attribute("name", init_gen=self.prod_name_gen)
        return product_formula.to_dataframe()

    def synthesize_and_push(self, size, conn, if_exists="replace", start=0):
        return self.push_to_db(self.synthesize_bucket(size, start), conn, if_exists)

    @staticmethod
    def push_to_db(product_data_frame, conn, if_exists="replace"):
        product_data_frame.to_sql("product", conn, if_exists=if_exists, index_label="product_id")
        return len(product_data_frame), product_data_frame.index.to_list()
