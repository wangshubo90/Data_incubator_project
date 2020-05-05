#!/home/spl/ml/data_incubator/bin/python
# Python version: 3.7.6

import pandas as pd 
import numpy as np 
import os 
import concurrent.futures

# read data and select necessary columns. 
df = pd.read_csv('PartD_Prescriber_PUF_NPI_17.txt', sep = '\t', keep_default_na= False, na_values=None)
column_selection = ['npi', 'bene_count','total_claim_count', 'total_day_supply',
                    'brand_claim_count','specialty_description','opioid_bene_count',
                    'antibiotic_bene_count','nppes_provider_state',
                    'total_claim_count_ge65','lis_claim_count',
                    'opioid_day_supply', 'opioid_claim_count','total_drug_cost'
                    ]
df = df[column_selection]

# Question 1: The average number of beneficiaries per provider
bene_count_series = df['bene_count']
bene_count_series = bene_count_series[bene_count_series != ''].astype('int')
Q1 = bene_count_series.mean()
print('Q1 answer:' + '\n' + 'The average number of beneficiaries per provider is {:>.5f}'.format(Q1))

# Question 2: median of the length of the average prescription
ave_presciption_df = df.loc[:,('total_claim_count', 'total_day_supply')]
ave_presciption_df.loc[:,'ave_length'] = ave_presciption_df.loc[:,'total_day_supply'] / ave_presciption_df.loc[:,'total_claim_count']
Q2 = ave_presciption_df['ave_length'].median()
print('Q2 answer:' + '\n' + 'The median of average length of prescriptions is {:>.5f}'.format(Q2))

# Question 3: the standard deviation of the fractions of drug claims that are for brand-name drugs
brand_name_claim_df = df[['total_claim_count','brand_claim_count','specialty_description']]
brand_not_suppress = brand_name_claim_df['brand_claim_count'] != ''
brand_name_claim_df = brand_name_claim_df[brand_not_suppress].astype(
            {'total_claim_count': 'int',
            'brand_claim_count':'int' }
            )
brand_name_claim_df = brand_name_claim_df.groupby('specialty_description').sum()
total_claim_atleast_1k = brand_name_claim_df['total_claim_count'] >= 1000
brand_name_claim_df = brand_name_claim_df[total_claim_atleast_1k]
brand_name_claim_df['brand_fraction'] = brand_name_claim_df['brand_claim_count'] / \
                                        brand_name_claim_df['total_claim_count']
Q3 = brand_name_claim_df['brand_fraction'].std()
print('Q3 answer:' + '\n' + 'The standard deviation of the fractions is {:>.5f}'.format(Q3))

# Question 4: ratio of beneficiaries with opioid prescriptions to beneficiaries with antibiotics prescriptions
opioid_vs_antibiotic_df = df[['opioid_bene_count','antibiotic_bene_count', 'nppes_provider_state']]

opioid_vs_antibiotic_df = opioid_vs_antibiotic_df[
            (opioid_vs_antibiotic_df['opioid_bene_count'] != '') & 
            (opioid_vs_antibiotic_df['antibiotic_bene_count'] != '')
            ].astype(
            {'opioid_bene_count':'int',
            'antibiotic_bene_count': 'int'}
            )

opioid_vs_antibiotic_df = opioid_vs_antibiotic_df.groupby('nppes_provider_state').sum()
            
opioid_vs_antibiotic_df['ratio_opioid_to_antibiotic'] = opioid_vs_antibiotic_df['opioid_bene_count'] / \
                                                        opioid_vs_antibiotic_df['antibiotic_bene_count']

Q4 = opioid_vs_antibiotic_df['ratio_opioid_to_antibiotic'].max() - \
    opioid_vs_antibiotic_df['ratio_opioid_to_antibiotic'].min()
print('Q4 answer:' + '\n' + 'The difference between max and min ratios is {:>.5f}'.format(Q4))

# Question 5: fraction of claims for beneficiaries age >=65 & low-income subsidy
ge65_vs_lis_claim_df = df[['total_claim_count_ge65','lis_claim_count','total_claim_count']]

ge65_vs_lis_claim_df = ge65_vs_lis_claim_df[
    (ge65_vs_lis_claim_df['total_claim_count_ge65'] != '') & 
    (ge65_vs_lis_claim_df['lis_claim_count'] != '') &
    (ge65_vs_lis_claim_df['total_claim_count'] != '')
    ].astype('int')

ge65_vs_lis_claim_df['ge65_fraction'] = ge65_vs_lis_claim_df['total_claim_count_ge65'] / \
                                        ge65_vs_lis_claim_df['total_claim_count']
ge65_vs_lis_claim_df['lis_fraction'] = ge65_vs_lis_claim_df['lis_claim_count'] / \
                                        ge65_vs_lis_claim_df['total_claim_count']

Q5 = ge65_vs_lis_claim_df[['ge65_fraction', 'lis_fraction']].corr(method = 'pearson').iloc[0,1]
print('Q5 answer:' + '\n' + 'Pearson correlation coefficient between fractions of Ge65_claim and LIS_claim is {:>.5f}'.format(Q5))

# Question 6: find which states have surprisingly high supply of opioids
ave_len_opioid = df[['nppes_provider_state', 'specialty_description', 'opioid_day_supply', 'opioid_claim_count']]
ave_len_opioid = ave_len_opioid[
            (ave_len_opioid['opioid_day_supply'] != '') &
            (ave_len_opioid['opioid_claim_count'] != '')
            ].astype(
                    {'opioid_day_supply':'int',
                    'opioid_claim_count':'int'}
                    )   
ave_len_opioid['ave_len'] = ave_len_opioid['opioid_day_supply'] / \
                ave_len_opioid['opioid_claim_count']

provider_count_by_state = ave_len_opioid.groupby(['nppes_provider_state','specialty_description']).count()

ave_len_opioid_by_state = ave_len_opioid.groupby(['nppes_provider_state','specialty_description']).mean()
    
ave_len_opioid_by_state = ave_len_opioid_by_state[provider_count_by_state['ave_len'] >= 100]

across_all_mean = ave_len_opioid_by_state['ave_len'].mean()

ave_len_opioid_by_state = ave_len_opioid_by_state.groupby(level=0).mean()
ave_len_opioid_by_state['ratio'] = ave_len_opioid_by_state['ave_len'] / across_all_mean
Q6 = ave_len_opioid_by_state['ratio'].max()
print('Q6 answer:' + '\n' + 'Largest ratio of opioid prescription in each state to across-state average is {:>.5f}'.format(Q6))

# Question 7: Average inflation rate across all providers
df2016 = pd.read_csv('PartD_Prescriber_PUF_NPI_16.txt', sep = '\t', keep_default_na= False, na_values=None)

ave_daily_cost_2017_df = df[['npi', 'total_drug_cost', 'total_day_supply']]
ave_daily_cost_2016_df = df2016[['npi', 'total_drug_cost', 'total_day_supply']]

ave_daily_cost_2017_df['daily_cost_2017'] = ave_daily_cost_2017_df['total_drug_cost'] / \
                                            ave_daily_cost_2017_df['total_day_supply']

ave_daily_cost_2016_df['daily_cost_2016'] = ave_daily_cost_2016_df['total_drug_cost'] / \
                                            ave_daily_cost_2016_df['total_day_supply']

inner_merged = pd.merge(ave_daily_cost_2017_df[['npi','daily_cost_2017']],ave_daily_cost_2016_df[['npi','daily_cost_2016']], on = 'npi')
inner_merged['inflation'] = inner_merged['daily_cost_2017'] / \
                            inner_merged['daily_cost_2016'] -1.0
Q7 = inner_merged['inflation'].mean()
print('Q7 answer:' + '\n' + 'Average inflation rate across all providers is {:>.3f}%'.format(Q7*100))

# Question 8: The largest fraction of providers left their specialties between 2016 and 2017
provider_2016_df = df2016[['npi', 'specialty_description']]
provider_2017_df = df[['npi', 'specialty_description']]
provider_2016_df.columns = ['provider', 'specialty_16']
provider_2017_df.columns = ['provider', 'specialty_17']

merged_temp_df = pd.merge(provider_2016_df, provider_2017_df, on='provider')
provider_count_2016 = merged_temp_df.groupby('specialty_16').count()
merged_temp_df['changed'] = merged_temp_df['specialty_16'] != merged_temp_df['specialty_17']
merged_temp_df = merged_temp_df.groupby('specialty_16').sum().astype({'changed':'int'})
merged_temp_df['provider'] = provider_count_2016['provider'].astype('int')
merged_temp_df['fraction'] = merged_temp_df['changed'] / \
                            merged_temp_df['provider']

merged_temp_df = merged_temp_df[
                (merged_temp_df['provider'] >= 1000) & 
                (merged_temp_df['changed'] != merged_temp_df['provider'])
                ]
Q8 = merged_temp_df['fraction'].max()
print('Q8 answer:' + '\n' + 'The largest fraction of providers left their specialties between 2016 and 2017 is {:>.3f}%'.format(Q8*100))