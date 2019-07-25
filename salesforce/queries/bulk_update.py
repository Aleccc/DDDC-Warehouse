from salesforce.salesforce_login import sf_bulk


def update_dddc_on_meters(data):
    update_job = sf_bulk.create_update_job(object='Meters__c', contentType='CSV')
    sf_bulk.bulk_csv_operation(update_job, data)
    results = sf_bulk.get_bulk_csv_operation_results(update_job)
    return results
