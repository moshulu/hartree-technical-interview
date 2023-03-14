
import apache_beam as beam
from apache_beam.dataframe.frames import *
from apache_beam.options.pipeline_options import PipelineOptions

DATASET1_PATH = 'provided-files\\dataset1.csv'
DATASET2_PATH = 'provided-files\\dataset2.csv'

class csv_transform_dataset1(beam.DoFn):
    def process(self, element):
        invoice_id, legal_entity, counter_party, rating, status, value = element.split(',')
        return [{
            'invoice_id': int(invoice_id),
            'legal_entity': str(legal_entity),
            'counter_party': str(counter_party),
            'rating': float(rating),
            'status': str(status),
            'value': float(value),
        }]
    
class csv_transform_dataset2(beam.DoFn):
    def process(self, element):
        counter_party, tier = element.split(',')
        return [{
            'counter_party': str(counter_party),
            'tier': int(tier)
        }]
    
# methods from https://github.com/HocLengChung/Apache-Beam-Dataflow-for-public/blob/master/leftjoin-blogexample.py
class UnnestCoGrouped(beam.DoFn):
    def process(self, input_element, source_pipeline_name, join_pipeline_name):
        group_key, grouped_dict = input_element
        join_dictionary = grouped_dict[join_pipeline_name]
        source_dictionaries = grouped_dict[source_pipeline_name]
        for source_dictionary in source_dictionaries:
            try:
                source_dictionary.update(join_dictionary[0])
                yield source_dictionary
            except IndexError:  # found no join_dictionary
                yield source_dictionary


def run(dataset1_path, dataset2_path):
    pipeline_options = PipelineOptions()
    p = beam.Pipeline(options=pipeline_options)

    dataset1 = (p
        | 'Read dataset1' >> beam.io.ReadFromText(dataset1_path)
        | 'Convert dataset1 to csv' >> beam.ParDo(csv_transform_dataset1())
    )
    print(dataset1)

    dataset2 = (p
        | 'Read dataset2' >> beam.io.ReadFromText(dataset2_path)
        | 'Convert dataset2 to csv' >> beam.ParDo(csv_transform_dataset2())
    )
    print(dataset2)

    merged_df = ({'dataset1': dataset1, 'dataset2': dataset2}
                 | 'join dataframes' >> beam.CoGroupByKey()
                 | 'flatten' >> beam.ParDo(UnnestCoGrouped(), 'ds1', 'ds2')
                 | beam.Map(lambda row: beam.Row(legal_entity=row['legal_entity'],
                                                 counter_party=row['counter_party'],
                                                 rating=row['rating'],
                                                 status=row['status'],
                                                 value=row['value'],
                                                 tier=row['tier']))
                )
    
    print(merged_df)
    
    legal_entity_df = (merged_df
                       | 'groupby legal_entity' >> beam.GroupBy('legal_entity')
                            .aggregate_field('counter_party', lambda row: row.counter_party, 'counter_party')
                            .aggregate_field('tier', lambda row: row.tier, 'tier')
                            .aggregate_field('rating', max, 'max_rating_by_counter_party')
                            .aggregate_field(lambda x: x.value if x.status == 'ARAP' else 0, sum, 'sum_value_where_status_ARAP')
                            .aggregate_field(lambda x: x.value if x.status == 'ACCR' else 0, sum, 'sum_value_where_status_ACCR')
                       )
    
    counter_party_df = (merged_df
                 | 'groupby counter_party' >> beam.GroupBy('counter_party')
                 .aggregate_field('legal_entity', lambda row: row.legal_entity, 'legal_entity')
                 .aggregate_field('tier', lambda row: row.tier, 'tier')
                 .aggregate_field('rating', max, 'max_rating_by_counter_party')
                 .aggregate_field(lambda x: x.value if x.status == 'ARAP' else 0, sum, 'sum_value_where_status_ARAP')
                 .aggregate_field(lambda x: x.value if x.status == 'ACCR' else 0, sum, 'sum_value_where_status_ACCR')
    )

    tier_df = (merged_df
              | 'group by tier' >> beam.GroupBy('tier')
              .aggregate_field('legal_entity', lambda row: row.legal_entity, 'legal_entity')
              .aggregate_field('counter_party', lambda row: row.counter_party, 'counter_party')
              .aggregate_field('rating', max, 'max_rating_by_counter_party')
              .aggregate_field(lambda x: x.value if x.status == 'ARAP' else 0, sum, 'sum_value_where_status_ARAP')
              .aggregate_field(lambda x: x.value if x.status == 'ACCR' else 0, sum, 'sum_value_where_status_ACCR')
    )

    legalentity_counterparty_df = (merged_df
                      | 'group by legal then counter' >> beam.GroupBy('legal_entity', 'counter_party')
                      .aggregate_field('tier', lambda row: row.tier, 'tier')
                      .aggregate_field('rating', max, 'max_rating_by_counter_party')
                      .aggregate_field(lambda x: x.value if x.status == 'ARAP' else 0, sum, 'sum_value_where_status_ARAP')
                      .aggregate_field(lambda x: x.value if x.status == 'ACCR' else 0, sum, 'sum_value_where_status_ACCR')
    )

    output_df = ((legal_entity_df, counter_party_df, tier_df, legalentity_counterparty_df)
                 | beam.Flatten()
                 | beam.io.WriteToText('output/output_df_beam.csv')
                 )


if __name__ == "__main__":
    run(DATASET1_PATH, DATASET2_PATH)