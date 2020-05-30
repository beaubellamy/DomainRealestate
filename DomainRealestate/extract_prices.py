import pandas as pd
import numpy as np
import re


# Remove the dates from the price field
def remove_dates(df, pattern):
    df['date_in_price'] = df['listing.priceDetails.displayPrice'].str.findall(pattern, re.IGNORECASE)

    # Replace the date in the display price
    for idx, row in df.iterrows():
        if row['date_in_price']:
            removed_date = row['listing.priceDetails.displayPrice'].replace(row['date_in_price'][0],'')
            df.loc[idx, 'listing.priceDetails.displayPrice'] = removed_date

    df.drop(['date_in_price'], axis=1, inplace=True)

    return df

def remove_times(df, pattern):

    df['time_in_price'] = df['listing.priceDetails.displayPrice'].str.findall(pattern)

    if sum(df['time_in_price'].isnull()) == 0:
        df.drop(['time_in_price'], axis=1, inplace=True)
        return df

    # Replace the time in the display price
    mask = ~df['time_in_price'].str[0].isnull()
    df.loc[mask, 'listing.priceDetails.displayPrice'] = \
        df[mask]['listing.priceDetails.displayPrice'].str.replace(df['time_in_price'][df[mask].index[0]][0],'')

    df.drop(['time_in_price'], axis=1, inplace=True)

    return df


def extend_numbers(df, pattern, delimiter=' '):

    df['alt'] = df['listing.priceDetails.displayPrice'].str.findall(pattern, flags=re.IGNORECASE)    

    if sum(df['alt'].isnull()) == 0:
        df.drop(['alt'], axis=1, inplace=True)
        return df

    df['float_value'] = df['alt'].str[0].str.split(delimiter).str[0].astype(float)*1e6
    df['replace_value'] = df['float_value'].fillna(0).astype(int)
    
    for idx, row in df.iterrows():

        if row['alt']:
            extend_number = row['listing.priceDetails.displayPrice'].replace(row['alt'][0],str(row[f'replace_value']))
            df.loc[idx, 'listing.priceDetails.displayPrice'] = extend_number

    df.drop(['alt', 'float_value', 'replace_value'], axis=1, inplace=True)
    
    return df


def listing_prices(filename):

    df = pd.read_csv(filename, sep=',')
    df.drop(['Unnamed: 0'], axis=1, inplace=True)

    # Todo: Make sure all listings have a real price
    # extract prices where available.
    # identify listings with no price and then price range function
    df['listing.priceDetails.displayPrice'] = df['listing.priceDetails.displayPrice'].fillna('none')
    null_price = df['listing.priceDetails.price'].isnull()

    # If the display price feature has a number, this is likely to be the price or dates
    # This is not expected to happen too often.
    display_is_number = df['listing.priceDetails.displayPrice'].str.isdigit()
    df.loc[(null_price & display_is_number), 'listing.priceDetails.price'] = \
        df[null_price & display_is_number]['listing.priceDetails.displayPrice']

    # Replace the time and date parts of the display price before finding the price, so the numbers are only price
    # Remove the numbers related to time
    time_pattern = '[0-9]{1,2}\.[0-9]{1,2}[ap]'
    df = remove_times(df, time_pattern)

    time_pattern = '\d:\d{2}' # 2:30-3:00pm
    df = remove_times(df, time_pattern)

    # Remove the numbers related to dates (May 30)
    date_pattern = r'(?=Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) \d{1,2}'
    df = remove_dates(df, date_pattern)

    # (30 May)
    date_pattern = r'\d{1,2} (?=Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)'
    df = remove_dates(df, date_pattern)

    # 1st, 2nd, 30th
    date_pattern = '[0-9]{1,2}[snrt]'
    df = remove_dates(df, date_pattern)

    # Remove other phrases with numbers in them
    # eg: 'sold in 7 days' or 'land 999m2'
    pattern = '\d{1,2} (?=day)'
    df = remove_dates(df, pattern)

    pattern = '\d{2,4}\w2'
    df = remove_dates(df, pattern)

    pattern = r'(\d{1}.{1,3} mill)'
    df = extend_numbers(df, pattern)

    pattern = r'\d{1,2} mill'
    df = extend_numbers(df, pattern)

    pattern = r'\d.\d{1,2}M$'
    df = extend_numbers(df, pattern, delimiter='M')

    # Create clean price features
    df['displayPrice'] = df['listing.priceDetails.displayPrice'].str.findall(r'([0-9-]{1,3})').str.join(sep='')

    df['fromPrice'] = df['listing.priceDetails.priceFrom']
    df['toPrice'] = df['listing.priceDetails.priceTo']

    # Seperate clean prices
    df.loc[null_price, 'fromPrice'] = df[null_price]['displayPrice'].str.split('-').str[0]
    df.loc[null_price, 'toPrice'] = df[null_price]['displayPrice'].str.split('-').str[1]

    null_toPrice = df['toPrice'].isnull()
    df.loc[null_toPrice, 'toPrice'] = df[null_toPrice]['fromPrice']

    empty_price = df['toPrice'] == ''
    df.loc[empty_price, 'toPrice'] = df[empty_price]['fromPrice']

    empty_price = df['fromPrice'] == ''
    df.loc[empty_price, 'fromPrice'] = df[empty_price]['toPrice']

    df.loc[df['toPrice'] == '', 'toPrice'] = np.nan
    df.loc[df['fromPrice'] == '', 'fromPrice'] = np.nan

    df.to_csv(filename)

    return df


#if __name__ == '__main__':

#    filename = 'local_listings.csv'

#    listing_prices(filename)


    