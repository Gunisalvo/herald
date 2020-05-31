from trumania.core.random_generators import SequencialGenerator
from .synthesizer import Synthesizer


class KycSynthesizer(Synthesizer):

    def __init__(self, env):
        super(KycSynthesizer, self).__init__(env)

    def synthesize_bucket(self, kyc_links, start=0):
        idx_gen = SequencialGenerator(prefix="KYC_", start=start)
        size = len(kyc_links)
        social_media_info_formula = self.dataset.create_population(name="kyc_%d" % start,
                                                                          size=size,
                                                                          ids_gen=idx_gen)

        df = social_media_info_formula.to_dataframe()
        df["kyc_link"] = kyc_links
        return df

    def synthesize_and_push(self, kyc_links, conn, if_exists="replace", start=0):
        return self.push_to_db(self.synthesize_bucket(kyc_links, start), conn, if_exists)

    @staticmethod
    def push_to_db(kyc_data_frame, conn, if_exists="replace"):
        kyc_data_frame.to_sql("kyc", conn, if_exists=if_exists, index_label="kyc_id")
        return len(kyc_data_frame)
