if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    print("rows before filtering:", len(data))
    print("Preprocessing: rows with zero or null passengers: ", 
        ((data['passenger_count'].isna()) | (data['passenger_count'] == 0)).sum())
    print("Preprocessing: rows with zero or null trip distance: ", 
        ((data['trip_distance'].isna()) | (data['trip_distance'] == 0)).sum())
    print("rows left:", len(data))
    
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    data.rename({
        'VendorID': 'vendor_id',
        'RatecodeID': 'ratec_ode_id',
        'PULocationID': 'pu_location_id',
        'DOLocationID': 'do_location_id'
        },
        axis=1, inplace=True)

    return data
