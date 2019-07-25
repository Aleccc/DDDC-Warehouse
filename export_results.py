import pandas as pd
from timer import timer
from salesforce.queries import bulk_update


df = pd.read_excel('static\\all_premises_in_sf.xlsx')
df = df[['Id', 'bucketed_or_year_prior']].rename(columns={'bucketed_or_year_prior': 'Forecasted_DDDC__c'})
print('done load')


@timer
def update(data_frame):
    # batch_size = 10000
    # list_of_dict = data_frame.to_dict('records')
    # for start in range(0, len(list_of_dict), batch_size):
    #     batch = list_of_dict[start:start+batch_size]
    #     result = bulk_update.update_dddc_on_meters(batch)
    #     print(start, result[0])
    result = bulk_update.update_dddc_on_meters(data_frame)
    result.to_csv('test.csv')

# update(df)
