from requests_html import HTML
from string import ascii_uppercase
import json
from tqdm import tqdm
import grequests
from copy import deepcopy


BASE_LINK = 'http://www.howstat.com/cricket/Statistics/Players/PlayerList.asp?Country=ALL&Group'
PLAYER_ODI_LINK = 'http://www.howstat.com/cricket/Statistics/Players/PlayerYears_ODI.asp'
ALL_YEARS_DATA = {}
BATCH_SIZE = 200


def divide_list(l, n):
    for i in range(0, len(l), n):  
        yield l[i:i + n]


def get_player_match_data(player_html):
    player_data = HTML(html=player_html)
    player_metadata = player_data.find('table')[4].find('tr')[1].find('td')[0].text.strip().split('(')
    player_name = player_metadata[0].strip()
    player_country = player_metadata[1].split(')')[0].strip()
    player_data_table = player_data.find('table')[5]
    player_match_years = player_data_table.find('tr')[3:-3]
    player_batting_data_years = player_match_years[:len(player_match_years) // 2 - 1]
    player_cummulative_runs = 0
    player_cummulative_matches = 0
    player_data = {}
    
    player_data['player_name'] = player_name
    player_data['player_country'] = player_country
    player_data['all_years_data'] = []
    for year in player_batting_data_years:
        year_dict = {}
        year_data = year.find('td')
        year_dict['year'] = int(year_data[0].text.strip())
        year_dict['year_matches'] = int(year_data[1].text.strip())
        player_cummulative_matches += year_dict['year_matches']
        year_dict['year_matches_cummulative'] = player_cummulative_matches
        year_dict['year_runs'] = int(year_data[8].text.strip())
        player_cummulative_runs += year_dict['year_runs']
        year_dict['year_runs_cummulative'] = player_cummulative_runs
        player_data['all_years_data'].append(year_dict)
        year_dict_copy = deepcopy(year_dict)
        year = int(year_dict_copy['year'])
        del year_dict_copy['year']
        year_dict_copy['player_name'] = player_name
        year_dict_copy['player_country'] = player_country
        if year in ALL_YEARS_DATA:
            ALL_YEARS_DATA[year].append(year_dict_copy)
        else:
            ALL_YEARS_DATA[year] = [year_dict_copy]
    
    return player_data


def get_players():
    all_players = []
    received_all_bool = False
    while not received_all_bool:
        player_list = (grequests.get(f'{BASE_LINK}={x}') for x in ascii_uppercase)
        player_list = grequests.map(player_list)
        received_all_bool =  all([bool(x.status_code==200) for x in player_list])
    
    for player_response in tqdm(player_list):
        player_data = HTML(html=player_response.text)
        player_table = player_data.find('table')[7]
        player_trs = player_table.find('tr')[2:-1]
        for tr in player_trs:
            player_tds = tr.find('td')
            if player_tds[4].text.strip():  # In some cases, the player has never played an ODI match, so avoid increasing network hits in that case
                player_code = player_tds[0].find('a')[0].attrs['href'].split('?')[-1]
                all_players.append(player_code)
            else:
                continue
    
    return all_players


def main():
    print('Collecting all players who have ever played ODI matches')
    all_players = get_players()
    print(f'Number of players found: {len(all_players)}')   
    all_players_data = []
    print(f'Processing individual player data in batches of: {BATCH_SIZE}')
    with tqdm(total=len(all_players)) as progress_bar:
        for batch in divide_list(all_players, BATCH_SIZE):
            received_all_bool = False
            while not received_all_bool:
                player_data_responses = (grequests.get(f'{PLAYER_ODI_LINK}?{player_code}') for player_code in batch)
                player_data_responses = grequests.map(player_data_responses)
                received_all_bool =  all([bool(x.status_code==200) for x in player_data_responses])
                # This stores a lot of data in the memory at runtime, but it's a tradeoff,
                # considering the data isn't big enough to need to put on disk and doing that increases overall speed
                for response in player_data_responses:
                    all_players_data.append(get_player_match_data(response.text))
                    progress_bar.update(1)
    print('Dumping JSON to disk')
    with open('all_player_data.json', 'w') as player_data_file:
        player_data_file.write(json.dumps(all_players_data, indent=4))
    with open('all_year_data.json', 'w') as player_data_file:
        player_data_file.write(json.dumps(ALL_YEARS_DATA, indent=4, sort_keys=True))
    print('All done!')

main()
