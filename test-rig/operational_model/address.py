from trumania.core.random_generators import SequencialGenerator, FakerGenerator
from .synthesizer import Synthesizer


class AddressSynthesizer(Synthesizer):

    def __init__(self, env):
        super(AddressSynthesizer, self).__init__(env)
        self.str_gen = FakerGenerator(method="street_name", seed=next(self.dataset.seeder))
        self.num_gen = FakerGenerator(method="building_number", seed=next(self.dataset.seeder))
        self.city_gen = FakerGenerator(method="city", seed=next(self.dataset.seeder))
        self.country_gen = FakerGenerator(method="bank_country", seed=next(self.dataset.seeder))

    def synthesize_bucket(self, user_fks, start=0):
        idx_gen = SequencialGenerator(prefix="ADDR_", start=start)
        addr_formula = self.dataset.create_population(name="address_%d" % start, size=len(user_fks),
                                                             ids_gen=idx_gen)
        addr_formula.create_attribute("street", init_gen=self.str_gen)
        addr_formula.create_attribute("number", init_gen=self.num_gen)
        addr_formula.create_attribute("city", init_gen=self.city_gen)
        addr_formula.create_attribute("county", init_gen=self.country_gen)
        df = addr_formula.to_dataframe()
        df["user_id"] = user_fks
        return df

    def synthesize_and_push(self, user_fks, conn, if_exists="replace", start=0):
        return self.push_to_db(self.synthesize_bucket(user_fks,start), conn, if_exists)

    @staticmethod
    def push_to_db(address_data_frame, conn, if_exists="replace"):
        address_data_frame.to_sql("address", conn, if_exists=if_exists, index_label="address_id")

        return len(address_data_frame)
