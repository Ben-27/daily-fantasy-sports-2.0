import requests
from io import StringIO

import pandas as pd
from bs4 import BeautifulSoup

def flatten_columns(df):
    """
    Concatenate multi-index columns.

    Args:
        - df: dataframe whose columns are to be flattened

    Returns:
        - cols: list of flattened column names
    """
    cols = []
    for a, b in df.columns:
        a = a if 'Unnamed' not in a else ''
        cols.append(' '.join([a, b]).strip())
    return cols

def initiate_session(login_url='https://fantasydata.com/user/login', email='benjamin.absalon@gmail.com', password='Element.27'):
    """
    Initiates a logged-on session of FantasyData.

    Returns:
        - session object if successful
        - None if not successful
    """
    # initiate session
    session = requests.Session()
    response = session.get(login_url)
    soup = BeautifulSoup(response.text, 'html.parser')

    # get the CSRF token
    csrf_token = soup.find('input', {'name': 'csrf_token'})['value']

    # set up payload and post to login_url
    payload = {'email': email, 'password': password, 'csrf_token': csrf_token}
    p = session.post(login_url, data=payload)
    
    if p.url != login_url:
        print('Successful')
        return session
    else:
        print('Not successful')
        return None

def get_stats_data(session, year, week_from, week_to, positions=['qb', 'rb', 'wr', 'te', 'dst']):
    """
    Gets data for given timeframe (year and weeks) and positions.

    Args:
        - session: logged in session
        - year: season
        - week_from: week to start from
        - week_to: week to end at
        - positions: list[str] of positions to be scraped

    Returns:
        - Dict[position: dataframe]
    """
    url = 'https://fantasydata.com/nfl/fantasy-football-leaders'

    data = {}

    # grab each position separately since distinct stats are recorded
    for position in positions:
        print(f'Grabbing data for {position.upper()}s...')
        pos_data = []

        # DST is weird in that is replicates the same page repetively
        # handle separately
        # TODO investigate possible fix
        if position=='dst':
            for week in range(week_from, week_to+1):
                params = {
                    'scope': 'game',
                    'sp': str(year) + '_REG',
                    'week_from': week,
                    'week_to': week,
                    'position': position,
                    'scoring': 'fpts_fanduel'
                }
                response = session.get(url, params=params)
                tables = pd.read_html(StringIO(response.text))
                
                pos_data.append(tables[0])

            # break when empty data (usually week 18 of an older year)
            if not tables or tables[0].empty:
                break

            # dst has single index columns, do not need flattened
            data[position] = pd.concat(pos_data)
            break

        # iterate through pages
        page_num = 1
        while True:
            params = {
                'scope': 'game',
                'sp': str(year) + '_REG',
                'week_from': week_from,
                'week_to': week_to,
                'position': position,
                'scoring': 'fpts_fanduel',
                'page': page_num
            }

            response = session.get(url, params=params)
            tables = pd.read_html(StringIO(response.text))

            # empty table means max pages reached
            if not tables or tables[0].empty:
                break
            
            # data is in first table
            pos_data.append(tables[0])
            page_num += 1
        
        # concatenate page tables, flatten columns, and append to data dictionary
        temp = pd.concat(pos_data)
        temp.columns = flatten_columns(temp)
        data[position] = temp
            
        print(f'   Complete!')

    return data

def get_projection_data(session, year, week_from, week_to, positions=['qb', 'rb', 'wr', 'te', 'dst']):
    """
    Gets projections for given timeframe (year and weeks) and positions.

    Args:
        - session: logged in session
        - year: season
        - week_from: week to start from
        - week_to: week to end at
        - positions: list[str] of positions to be scraped

    Returns:
        - Dict[position: dataframe]
    """
    url = 'https://fantasydata.com/nfl/fantasy-football-weekly-projections'

    data = {}

    # grab each position separately since distinct stats are recorded
    for position in positions:
        print(f'Grabbing data for {position.upper()}s...')
        pos_data = []
        for week in range(week_from, week_to+1):
            # iterate through pages
            page_num = 1
            while True:
                params = {
                    'scope': 'game',
                    'spw': str(year) + '_REG_' + str(week),
                    'position': position,
                    'scoring': 'fpts_fanduel',
                    'page': page_num
                }

                response = session.get(url, params=params)
                tables = pd.read_html(StringIO(response.text))

                # empty table means max pages reached
                if not tables or tables[0].empty:
                    break
                
                # data is in first table
                pos_data.append(tables[0])
                page_num += 1

                # DST is weird
                if position == 'dst':
                    break

        # if there is data 
        # concatenate page tables, flatten columns, and append to data dictionary
        if pos_data:
            temp = pd.concat(pos_data)
            if position!='dst':
                temp.columns = flatten_columns(temp)
            data[position] = temp
            
        print(f'   Complete!')

    return data

def get_salary_data(session, year, week_from, week_to):
    """
    Gets salaries for given timeframe (year and weeks).

    Args:
        - session: logged in session
        - year: season
        - week_from: week to start from
        - week_to: week to end at

    Returns:
        - dataframe
    """
    url = 'https://fantasydata.com/nfl/daily-fantasy-football-salary-and-projection-tool'

    data = []

    # grab each position separately since distinct stats are recorded
    for week in range(week_from, week_to+1):
        # iterate through pages
        page_num = 1
        while True:
            params = {
                'scope': 'game',
                'spw': str(year) + '_REG_' + str(week),
                'operator': 'fanduel',
                'page': page_num
            }

            response = session.get(url, params=params)
            tables = pd.read_html(StringIO(response.text))

            # empty table means max pages reached
            if not tables or tables[0].empty:
                break
            
            # data is in first table
            temp = tables[0]
            temp['WK'] = week
            data.append(temp)
            page_num += 1

    # columns have single layer and do not need flattened
    return pd.concat(data)


# s = initiate_session()
# get_projection_data(s, 2015, 1, 18, positions=['dst'])
# s.close()