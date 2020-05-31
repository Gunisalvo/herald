from trumania.core.random_generators import SequencialGenerator
from .synthesizer import Synthesizer
import random


class PayTypeSynthesizer(Synthesizer):

    def __init__(self, env, pay_type_per_user_ratio):
        super(PayTypeSynthesizer, self).__init__(env)
        self.pay_type_per_user_ratio = pay_type_per_user_ratio

    def synthesize_bucket(self, user_fks, start=0):
        idx_gen = SequencialGenerator(prefix="PAY_", start=start)
        size = random.randint(len(user_fks), len(user_fks) * self.pay_type_per_user_ratio)
        pay_type_formula = self.dataset.create_population(name="pay_type_%d" % start,
                                                                 size=size,
                                                                 ids_gen=idx_gen)
        df = pay_type_formula.to_dataframe()
        user_seed = []
        for _ in range(self.pay_type_per_user_ratio):
            user_seed.extend(user_fks)
        df["user_id"] = random.sample(user_seed, len(df))
        return df

    def synthesize_and_push(self, user_fks, conn, if_exists="replace", start=0):
        return self.push_to_db(self.synthesize_bucket(user_fks,start), conn, if_exists)

    @staticmethod
    def push_to_db(pay_type_data_frame, conn, if_exists="replace"):
        pay_type_data_frame.to_sql("payment_type", conn, if_exists=if_exists, index_label="pay_type_id")
        return len(pay_type_data_frame), pay_type_data_frame.index.to_list()
