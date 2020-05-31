from trumania.core.random_generators import SequencialGenerator, FakerGenerator
from .synthesizer import Synthesizer
import random


class PurchaseSynthesizer(Synthesizer):

    def __init__(self, env, per_pay_type_ratio):
        super(PurchaseSynthesizer, self).__init__(env)
        self.kyc_gen = FakerGenerator(method="ean", seed=next(self.dataset.seeder))
        self.per_pay_type_ratio = per_pay_type_ratio

    def synthesize_bucket(self, pay_type_fks, start=0):
        idx_gen = SequencialGenerator(prefix="PUR_", start=start)

        size = random.randint(len(pay_type_fks), len(pay_type_fks) * self.per_pay_type_ratio)
        purchase_formula = self.dataset.create_population(name="purchase_%d" % start,
                                                                 size=size,
                                                                 ids_gen=idx_gen)
        purchase_formula.create_attribute("kyc", init_gen=self.kyc_gen)
        df = purchase_formula.to_dataframe()
        pay_type_seed = []
        for _ in range(self.per_pay_type_ratio):
            pay_type_seed.extend(pay_type_fks)
        df["pay_type_id"] = random.sample(pay_type_seed, len(df))
        return df

    def synthesize_and_push(self, pay_type_fks, conn, if_exists="replace", start=0):
        return self.push_to_db(self.synthesize_bucket(pay_type_fks, start), conn, if_exists)

    @staticmethod
    def push_to_db(purchase_data_frame, conn, if_exists="replace"):
        purchase_data_frame.to_sql("purchase", conn, if_exists=if_exists, index_label="purchase_id")
        return len(purchase_data_frame), purchase_data_frame.index.to_list(), purchase_data_frame["kyc"].to_list()
