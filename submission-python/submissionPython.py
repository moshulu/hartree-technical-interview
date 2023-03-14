import numpy as np
import pandas as pd

dataset1 = pd.read_csv('../provided-files/dataset1.csv')
dataset2 = pd.read_csv('../provided-files/dataset2.csv')

# got here
merged_df = dataset1.merge(dataset2, on='counter_party')

# transitory df, this gets the max value of 'value' based on the unique values of 'counter_party'
cp_rating_df = merged_df.groupby('counter_party')['rating'].agg(max)

# merge it back into the big df, based on counter_party col
merged_df = merged_df.merge(cp_rating_df, on='counter_party')
# there will be 2 'rating' columns now, so rename it to what we want
merged_df = merged_df.rename(columns={
    'rating_y': 'max(rating by counterparty)',
    'rating_x': 'rating'
})

# get the value of 'value' where status = what we want. There's no summing involved, I'm just reading the instructions provided in provided-files/sample_test.txt.
merged_df['sum(value where status=ARAP)'] = np.where(merged_df['status'] == "ARAP", merged_df['value'], 0)
merged_df['sum(value where status=ACCR)'] = np.where(merged_df['status'] == "ACCR", merged_df['value'], 0)

legal_entity_df = merged_df.groupby('legal_entity').agg({'legal_entity': 'first', # gets the first non-null entry of each column
                                       'counter_party': 'first', # unique elements in each position
                                       'tier': 'first', # unique elements in each position
                                       'max(rating by counterparty)': 'max', # compute max of group values
                                       'sum(value where status=ARAP)': 'sum', # compute sum of group values
                                       'sum(value where status=ACCR)': 'sum'}) # compute sum of group values

counter_party_df = merged_df.groupby(['counter_party']).agg({
                                          'legal_entity': 'first',
                                          'counter_party': 'first',
                                          'tier': 'first',
                                          'max(rating by counterparty)': 'max',
                                          'sum(value where status=ARAP)': 'sum',
                                          'sum(value where status=ACCR)': 'sum'})

tier_df = merged_df.groupby(['tier']).agg({'legal_entity': 'first',
                                 'counter_party': 'first',
                                 'tier': 'first',
                                 'max(rating by counterparty)': 'max',
                                 'sum(value where status=ARAP)': 'sum',
                                 'sum(value where status=ACCR)': 'sum'})

le_cp_df = merged_df.groupby(['legal_entity', 'counter_party']).agg({'legal_entity': 'first',
                                                           'counter_party': 'first',
                                                           'tier': 'first',
                                                           'sum(value where status=ARAP)': 'sum',
                                                           'sum(value where status=ACCR)': 'sum',
                                                           'max(rating by counterparty)': 'max'})


output_df = pd.concat([legal_entity_df, counter_party_df, tier_df, le_cp_df]).reset_index(drop=True)
output_df.to_csv('../output-files/output_df.csv', index=False)

output_df