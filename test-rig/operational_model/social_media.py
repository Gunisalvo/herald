from trumania.core.random_generators import SequencialGenerator
from .synthesizer import Synthesizer


class SocialMediaSynthesizer(Synthesizer):

    def __init__(self, env):
        super(SocialMediaSynthesizer, self).__init__(env)

    def synthesize_bucket(self, social_media_links, start=0):
        idx_gen = SequencialGenerator(prefix="SM_", start=start)
        size = len(social_media_links)
        social_media_info_formula = self.dataset.create_population(name="social_media_info_%d" % start,
                                                                          size=size,
                                                                          ids_gen=idx_gen)
        df = social_media_info_formula.to_dataframe()
        df["social_link"] = social_media_links
        return df

    def synthesize_and_push(self, social_media_links, conn, if_exists="replace", start=0):
        return self.push_to_db(self.synthesize_bucket(social_media_links, start), conn, if_exists)

    @staticmethod
    def push_to_db(social_media_data_frame, conn, if_exists="replace"):
        social_media_data_frame.to_sql("social_media", conn, if_exists=if_exists, index_label="sm_id")
        return len(social_media_data_frame)
