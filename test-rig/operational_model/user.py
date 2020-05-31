from trumania.core.random_generators import SequencialGenerator, FakerGenerator, NumpyRandomGenerator
from .synthesizer import Synthesizer


class UserSynthesizer(Synthesizer):

    def __init__(self, env, missing_social_media_ratio):
        super(UserSynthesizer, self).__init__(env)
        self.missing_social_media_ratio = missing_social_media_ratio
        self.age_gen = NumpyRandomGenerator(
            method="gamma",
            shape=15,
            scale=3,
            seed=next(self.dataset.seeder)).map(f=lambda x: int(x))
        self.name_gen = FakerGenerator(method="name", seed=next(self.dataset.seeder))
        self.social_gen = FakerGenerator(method="ean", seed=next(self.dataset.seeder))

    def synthesize_bucket(self, size, start=0):
        idx_gen = SequencialGenerator(prefix="USR_", start=start)
        user_formula = self.dataset.create_population(name="user_%d" % start, size=size, ids_gen=idx_gen)
        user_formula.create_attribute("full_name", init_gen=self.name_gen)
        user_formula.create_attribute("age", init_gen=self.age_gen)
        user_formula.create_attribute("social", init_gen=self.social_gen)
        df = user_formula.to_dataframe()
        df.loc[
            df.sample(frac=self.missing_social_media_ratio).index,
            "social"
        ] = None
        return df

    def synthesize_and_push(self, size, conn, if_exists="replace", start=0):
        return self.push_to_db(self.synthesize_bucket(size,start), conn, if_exists)

    @staticmethod
    def push_to_db(user_data_frame, conn, if_exists="replace"):
        user_data_frame.to_sql("user", conn, if_exists=if_exists, index_label="user_id")
        user_fk = user_data_frame.index.values
        social_links = user_data_frame[(user_data_frame["social"].notnull())]["social"].to_list()

        return len(user_data_frame), user_fk, social_links
